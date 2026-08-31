package com.purbon.kafka.topology.actions.access.builders;

import com.purbon.kafka.topology.BindingsBuilderProvider;
import com.purbon.kafka.topology.model.users.MirrorMaker2;

public class MirrorMaker2AclBindingsBuilder implements AclBindingsBuilder {

  private final BindingsBuilderProvider builderProvider;
  private final MirrorMaker2 mirrorMakerInstance;

  public MirrorMaker2AclBindingsBuilder(
      BindingsBuilderProvider builderProvider, MirrorMaker2 mirrorMakerInstance) {
    this.builderProvider = builderProvider;
    this.mirrorMakerInstance = mirrorMakerInstance;
  }

  @Override
  public AclBindingsResult getAclBindings() {
    return AclBindingsResult.forAclBindings(
        builderProvider.buildBindingForMirrorMaker2(mirrorMakerInstance));
  }
}
