// Copyright (c) The OpenTofu Authors
// SPDX-License-Identifier: MPL-2.0
// Copyright (c) 2023 HashiCorp, Inc.
// SPDX-License-Identifier: MPL-2.0

package planning

import (
	"context"
	"fmt"
	"sync"

	"github.com/opentofu/opentofu/internal/addrs"
	"github.com/opentofu/opentofu/internal/engine/internal/execgraph"
	"github.com/opentofu/opentofu/internal/lang/eval"
	"github.com/opentofu/opentofu/internal/shared"
	"github.com/opentofu/opentofu/internal/tfdiags"
	"github.com/zclconf/go-cty/cty"
)

// ephemeralInstances is our central manager of active configured ephemeral
// instances, responsible for executing new ephemerals on request and for
// keeping them running until all of their work is done.
type ephemeralInstances struct {
	closeBlockersMu sync.Mutex
	closeBlockers   addrs.Map[addrs.AbsResourceInstance, execgraph.RegisterCloseBlockerFunc]
}

func newEphemeralInstances() *ephemeralInstances {
	return &ephemeralInstances{
		closeBlockers: addrs.MakeMap[addrs.AbsResourceInstance, execgraph.RegisterCloseBlockerFunc](),
	}
}

func (e *ephemeralInstances) addCloseDependsOn(addr addrs.AbsResourceInstance, dep execgraph.AnyResultRef) {
	e.closeBlockersMu.Lock()
	registerCloseBlocker := e.closeBlockers.Get(addr)
	e.closeBlockersMu.Unlock()

	if registerCloseBlocker != nil {
		registerCloseBlocker(dep)
	}
}

func (p *planGlue) planDesiredEphemeralResourceInstance(ctx context.Context, inst *eval.DesiredResourceInstance, egb *execgraph.Builder) (cty.Value, execgraph.ResourceInstanceResultRef, tfdiags.Diagnostics) {
	var diags tfdiags.Diagnostics

	schema, _ := p.planCtx.providers.ResourceTypeSchema(ctx, inst.Provider, inst.Addr.Resource.Resource.Mode, inst.Addr.Resource.Resource.Type)
	if schema == nil || schema.Block == nil {
		// Should be caught during validation, so we don't bother with a pretty error here
		diags = diags.Append(fmt.Errorf("provider %q does not support ephemeral resource %q", inst.ProviderInstance, inst.Addr.Resource.Resource.Type))
		return cty.NilVal, nil, diags
	}

	if inst.ProviderInstance == nil {
		// If we don't even know which provider instance we're supposed to be
		// talking to then we'll just return a placeholder value, because
		// we don't have any way to generate a speculative plan.
		return cty.NilVal, nil, diags
	}

	providerClient, providerClientRef, closeProviderAfter, moreDiags := p.providerClient(ctx, *inst.ProviderInstance)
	if providerClient == nil {
		moreDiags = moreDiags.Append(tfdiags.AttributeValue(
			tfdiags.Error,
			"Provider instance not available",
			fmt.Sprintf("Cannot plan %s because its associated provider instance %s cannot initialize.", inst.Addr, *inst.ProviderInstance),
			nil,
		))
	}
	diags = diags.Append(moreDiags)
	if moreDiags.HasErrors() {
		return cty.NilVal, nil, diags
	}

	newVal, closeFunc, openDiags := shared.OpenEphemeralResourceInstance(
		ctx,
		inst.Addr,
		schema.Block,
		*inst.ProviderInstance,
		providerClient,
		inst.ConfigVal,
		shared.EphemeralResourceHooks{},
	)
	diags = diags.Append(openDiags)
	if openDiags.HasErrors() {
		return cty.NilVal, nil, diags
	}

	dependencyResults := make([]execgraph.AnyResultRef, 0, len(inst.RequiredResourceInstances))
	for _, depInstAddr := range inst.RequiredResourceInstances {
		depInstResult := egb.ResourceInstanceFinalStateResult(depInstAddr)
		dependencyResults = append(dependencyResults, depInstResult)
	}
	dependencyWaiter := egb.Waiter(dependencyResults...)

	instAddrRef := egb.ConstantResourceInstAddr(inst.Addr)
	desiredInstRef := egb.ResourceInstanceDesired(instAddrRef, dependencyWaiter)
	openRef := egb.EphemeralOpen(desiredInstRef, providerClientRef)

	closeWait, registerCloseBlocker := egb.MakeCloseBlocker()
	closeRef := egb.EphemeralClose(openRef, providerClientRef, closeWait)

	for _, depInstAddr := range inst.RequiredResourceInstances {
		if depInstAddr.Resource.Resource.Mode == addrs.EphemeralResourceMode {
			// Our open was dependent on an ephemeral's open,
			// therefore the ephemeral's close should depend on our close
			//
			// The dependency should already have been populated via planDesiredEphemeralResourceInstance
			p.planCtx.ephemeralInstances.addCloseDependsOn(depInstAddr, closeRef)
		}
	}

	closeProviderAfter(closeRef)

	p.planCtx.ephemeralInstances.closeBlockersMu.Lock()
	p.planCtx.ephemeralInstances.closeBlockers.Put(inst.Addr, registerCloseBlocker)
	p.planCtx.ephemeralInstances.closeBlockersMu.Unlock()

	p.planCtx.closeStackMu.Lock()
	p.planCtx.closeStack = append(p.planCtx.closeStack, closeFunc)
	p.planCtx.closeStackMu.Unlock()

	return newVal, openRef, diags
}
