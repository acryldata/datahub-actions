# Tag Sync Action

The Tag Sync (or Tag Propagation) Action allows you to propagate tags from your assets into downstream entities. e.g. You can apply a tag (like `critical`) on a dataset and have it propagate down to all the downstream datasets.


## Configurability

You can control which tags should be propagated downstream using a prefix system. E.g. You can specify that only tags that start with `tier:` should be propagated downstream.

## Caveats

- Tag Propagation is currently only supported for downstream datasets. Tags will not propagate to downstream dashboards or charts. Let us know if this is an important feature for you.
- Tag Sync is currently only "additive". Removing the tag from the upstream dataset will not propagate the removal down to downstream datasets. Let us know if this is an important feature for you.
