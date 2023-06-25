# Tag Sync Action

The Tag Sync (or Tag Propagation) Action allows you to propagate tags from your assets into downstream entities. e.g. You can apply a tag (like `critical`) on a dataset and have it propagate down to all the downstream datasets.


## Configurability

You can control which tags should be propagated downstream using a prefix system. E.g. You can specify that only tags that start with `tier:` should be propagated downstream.

## Additions and Removals

The action supports both additions and removals of tags.

## Caveats

- Tag Propagation is currently only supported for downstream datasets. Tags will not propagate to downstream dashboards or charts. Let us know if this is an important feature for you.