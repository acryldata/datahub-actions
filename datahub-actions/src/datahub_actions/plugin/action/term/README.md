# Glossary Term Propagation Action

The Glossary Term Propagation Action allows you to propagate glossary terms from your assets into downstream entities. 

## Use Cases

Enable classification of datasets or field of datasets to taint downstream datasets with minimum manual work.

## Functionality 

- Given a "target term", the propagation action will detect application of the target term to any field or dataset and propagate it down (as a dataset-level tag) on all downstream datasets. For example, given a target term of `Classification.Confidential` (the default), if you apply `Classification.Confidential` term to a dataset (at the dataset level or a field-level), this action will find all the downstream datasets and apply the `Classification.Confidential` tag to them at the dataset level. Note that downstream application is only at the dataset level, regardless of whether the primary application was at the field level or the dataset level. 
- This action also supports term linkage. If you apply a term that is linked to the target term via inheritance, then this action will detect that application and propagate it downstream as well. For example, if the term `PersonalInformation.Email` inherits `Classification.Confidential` (the target term), and if you apply the `PersonalInformation.Email` term to a dataset (or a field in the dataset), it will be picked up by the action, and the `Classification.Confidential` term will be applied at the dataset level to all the downstream entities. 

## Configurability

You can control what the target term should be. The default is `Classification.Confidential`.
Linkage to the target term is controlled through your business glossary which is completely under your control.

## Caveats

- Term Propagation is currently only supported for downstream datasets. Terms will not propagate to downstream dashboards or charts. Let us know if this is an important feature for you.
- Term Propagation is currently only "additive". Removing the term from the upstream dataset will not propagate the removal down to downstream datasets. Let us know if this is an important feature for you.
