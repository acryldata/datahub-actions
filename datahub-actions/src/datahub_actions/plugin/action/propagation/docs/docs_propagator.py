import json
import logging
import time
from typing import Optional

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub_actions.api.action_graph import AcrylDataHubGraph
from datahub_actions.event.event_envelope import EventEnvelope
from datahub_actions.event.event_registry import EntityChangeEvent
from datahub_actions.plugin.action.propagation.docs.propagation_action import DocPropagationDirective
from datahub_actions.plugin.action.propagation.propagation_utils import SourceDetails, PropagationDirective
from datahub.utilities.urns.urn import guess_entity_type
from datahub.utilities.urns.urn import Urn
from datahub.metadata.urns import DatasetUrn

from datahub.metadata.schema_classes import (
    AuditStampClass,
    DocumentationAssociationClass,
    DocumentationClass,
    GenericAspectClass,
    MetadataAttributionClass,

)

from datahub_actions.plugin.action.propagation.propagator import EntityPropagatorConfig, EntityPropagator

logger = logging.getLogger(__name__)

class DocsPropagatorConfig(EntityPropagatorConfig):
    columns_enabled: bool = True

class DocsPropagator(EntityPropagator):
    def __init__(self, action_urn: str, graph: AcrylDataHubGraph, config: DocsPropagatorConfig) -> None:
        super().__init__(action_urn, graph, config)
        self.config = config
        self.graph = graph
        self.actor_urn = action_urn

        self.mcl_processor.register_processor(
            entity_type="schemaField",
            aspect="DOCUMENTATION",
            processor=self.process_schema_field_documentation_mcl,
        )

        self.mce_processor.register_processor(
            category="DOCUMENTATION",
            entity_type="schemaField",
            processor=self.process_mce,
        )

    def process_schema_field_documentation_mcl(
            self,
            entity_urn: str,
            aspect_name: str,
            aspect_value: GenericAspectClass,
            previous_aspect_value: Optional[GenericAspectClass],
    ) -> Optional[DocPropagationDirective]:
        """
        Process changes in the documentation aspect of schemaField entities.
        Produce a directive to propagate the documentation.
        Business Logic checks:
            - If the documentation is sourced by this action, then we propagate
              it.
            - If the documentation is not sourced by this action, then we log a
                warning and propagate it.
            - If we have exceeded the maximum depth of propagation or maximum
              time for propagation, then we stop propagation and don't return a directive.
        """
        assert isinstance(self.config,DocsPropagatorConfig)
        logger.info("Process MCL from DocPropagation")

        if (
                aspect_name != "documentation"
                or guess_entity_type(entity_urn) != "schemaField"
        ):
            # not a documentation aspect or not a schemaField entity
            return None

        logger.info("Processing 'documentation' MCL")
        if self.config.columns_enabled:
            current_docs = DocumentationClass.from_obj(json.loads(aspect_value.value))
            old_docs = (
                None
                if previous_aspect_value is None
                else DocumentationClass.from_obj(
                    json.loads(previous_aspect_value.value)
                )
            )
            if current_docs.documentations:
                # get the most recently updated documentation with attribution
                current_documentation_instance = sorted(
                    [doc for doc in current_docs.documentations if doc.attribution],
                    key=lambda x: x.attribution.time if x.attribution else 0,
                )[-1]
                assert current_documentation_instance.attribution
                if (
                        current_documentation_instance.attribution.source is None
                        or current_documentation_instance.attribution.source
                        != self.action_urn
                ):
                    logger.warning(
                        f"Documentation is not sourced by this action which is unexpected. Will be propagating for {entity_urn}"
                    )
                source_details = (
                    (current_documentation_instance.attribution.sourceDetail)
                    if current_documentation_instance.attribution
                    else {}
                )
                source_details_parsed: SourceDetails = SourceDetails.parse_obj(
                    source_details
                )
                should_stop_propagation, reason = self.should_stop_propagation(
                    source_details_parsed
                )
                if should_stop_propagation:
                    logger.warning(f"Stopping propagation for {entity_urn}. {reason}")
                    return None
                else:
                    logger.debug(f"Propagating documentation for {entity_urn}")
                propagation_relationships = self.get_propagation_relationships(source_details=source_details_parsed
                )
                origin_entity = (
                    source_details_parsed.origin
                    if source_details_parsed.origin
                    else entity_urn
                )
                if old_docs is None or not old_docs.documentations:
                    return DocPropagationDirective(
                        propagate=True,
                        doc_string=current_documentation_instance.documentation,
                        operation="ADD",
                        entity=entity_urn,
                        origin=origin_entity,
                        via=entity_urn,
                        actor=self.actor_urn,
                        propagation_started_at=source_details_parsed.propagation_started_at,
                        propagation_depth=(
                            source_details_parsed.propagation_depth + 1
                            if source_details_parsed.propagation_depth
                            else 1
                        ),
                        relationships=propagation_relationships,
                    )
                else:
                    old_docs_instance = sorted(
                        old_docs.documentations,
                        key=lambda x: x.attribution.time if x.attribution else 0,
                    )[-1]
                    if (
                            current_documentation_instance.documentation
                            != old_docs_instance.documentation
                    ):
                        return DocPropagationDirective(
                            propagate=True,
                            doc_string=current_documentation_instance.documentation,
                            operation="MODIFY",
                            entity=entity_urn,
                            origin=origin_entity,
                            via=entity_urn,
                            actor=self.actor_urn,
                            propagation_started_at=source_details_parsed.propagation_started_at,
                            propagation_depth=(
                                source_details_parsed.propagation_depth + 1
                                if source_details_parsed.propagation_depth
                                else 1
                            ),
                            relationships=propagation_relationships,
                        )
        return None

    def process_mce(self, event: EventEnvelope) -> Optional[DocPropagationDirective]:
        assert isinstance(event.event, EntityChangeEvent)
        assert self.graph is not None
        assert isinstance(self.config,DocsPropagatorConfig)

        semantic_event = event.event
        if (
                semantic_event.category == "DOCUMENTATION"
                and self.config is not None
                and self.config.enabled
        ):
            logger.info(f"MCE from DocPropagation: {event}")
            if self.config.columns_enabled and (
                    semantic_event.entityType == "schemaField"
            ):
                if semantic_event.parameters:
                    parameters = semantic_event.parameters
                else:
                    parameters = semantic_event._inner_dict.get(
                        "__parameters_json", {}
                    )
                doc_string = parameters.get("description")
                origin = parameters.get("origin")
                origin = origin or semantic_event.entityUrn
                via = (
                    semantic_event.entityUrn
                    if origin != semantic_event.entityUrn
                    else None
                )
                logger.debug(f"Origin: {origin}")
                logger.debug(f"Via: {via}")
                logger.debug(f"Doc string: {doc_string}")
                logger.debug(f"Semantic event {semantic_event}")
                if doc_string:
                    return DocPropagationDirective(
                        propagate=True,
                        doc_string=doc_string,
                        operation=semantic_event.operation,
                        entity=semantic_event.entityUrn,
                        origin=origin,
                        via=via,  # if origin is set, then via is the entity itself
                        actor=(
                            semantic_event.auditStamp.actor
                            if semantic_event.auditStamp
                            else self.actor_urn
                        ),
                        propagation_started_at=int(time.time() * 1000.0),
                        propagation_depth=1,  # we start at 1 because this is the first propagation
                        relationships=self.get_propagation_relationships(
                            source_details=None,
                        ),
                    )
        return None

    def modify_docs_on_columns(
            self,
            graph: AcrylDataHubGraph,
            operation: str,
            schema_field_urn: str,
            dataset_urn: str,
            field_doc: Optional[str],
            context: SourceDetails,
    ) -> Optional[MetadataChangeProposalWrapper]:
        if context.origin == schema_field_urn:
            # No need to propagate to self
            logger.info("Skipping documentation propagation to self")
            return None

        try:
            DatasetUrn.from_string(dataset_urn)
        except Exception as e:
            logger.error(
                f"Invalid dataset urn {dataset_urn}. {e}. Skipping documentation propagation."
            )
            return None

        auditStamp = AuditStampClass(
            time=int(time.time() * 1000.0), actor=self.actor_urn
        )

        source_details = context.for_metadata_attribution()
        attribution: MetadataAttributionClass = MetadataAttributionClass(
            source=self.action_urn,
            time=auditStamp.time,
            actor=self.actor_urn,
            sourceDetail=source_details,
        )
        documentations = graph.graph.get_aspect(schema_field_urn, DocumentationClass)
        if documentations:
            logger.info(f"Found existing documentation {documentations} for {schema_field_urn}")
            mutation_needed = False
            action_sourced = False
            # we check if there are any existing documentations generated by
            # this action and sourced from the same origin, if so, we update them
            # otherwise, we add a new documentation entry sourced by this action
            for doc_association in documentations.documentations[:]:
                if doc_association.attribution and doc_association.attribution.source:
                    source_details_parsed: SourceDetails = SourceDetails.parse_obj(
                        doc_association.attribution.sourceDetail
                    )
                    if doc_association.attribution.source == self.action_urn and (
                            source_details_parsed.origin == context.origin
                    ):
                        action_sourced = True
                        if doc_association.documentation != field_doc:
                            mutation_needed = True
                            if operation == "ADD" or operation == "MODIFY":
                                doc_association.documentation = field_doc or ""
                                doc_association.attribution = attribution
                            elif operation == "REMOVE":
                                documentations.documentations.remove(doc_association)
            if not action_sourced:
                documentations.documentations.append(
                    DocumentationAssociationClass(
                        documentation=field_doc or "",
                        attribution=attribution,
                    )
                )
                mutation_needed = True
        else:
            # no docs found, create a new one
            # we don't check editableSchemaMetadata because our goal is to
            # propagate documentation to downstream entities
            # UI will handle resolving priorities and conflicts
            documentations = DocumentationClass(
                documentations=[
                    DocumentationAssociationClass(
                        documentation=field_doc or "",
                        attribution=attribution,
                    )
                ]
            )
            mutation_needed = True

        if mutation_needed:
            logger.info(
                f"Will emit documentation {documentations} change proposal for {schema_field_urn} with {field_doc}"
            )
            return MetadataChangeProposalWrapper(
                entityUrn=schema_field_urn,
                aspect=documentations,
            )
        else:
            logger.info(f"No mutation needed for {schema_field_urn}")
        return None


    def create_property_change_proposal(
            self,
            propagation_directive: PropagationDirective,
            entity_urn: Urn,
            context: SourceDetails
    ) -> Optional[MetadataChangeProposalWrapper]:
        assert isinstance(propagation_directive, DocPropagationDirective)

        parent_urn = entity_urn.entity_ids[0]

        return self.modify_docs_on_columns(
            graph=self.graph,
            operation=propagation_directive.operation,
            schema_field_urn=str(entity_urn),
            dataset_urn=parent_urn,
            field_doc=propagation_directive.doc_string,
            context=context,
        )

