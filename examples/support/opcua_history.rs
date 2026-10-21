use opcua::nodes::AccessLevel;
use opcua::server::{
    ContinuationPoint,
    address_space::{AddressSpace, VariableBuilder},
    diagnostics::NamespaceMetadata,
    node_manager::{
        HistoryNode, NodeManagerBuilder, RequestContext, ServerContext,
        memory::{InMemoryNodeManager, InMemoryNodeManagerBuilder, InMemoryNodeManagerImpl},
    },
};
use opcua::types::{
    DataTypeId, DataValue, HistoryData, NodeId, ObjectId, ReadRawModifiedDetails, StatusCode,
    TimestampsToReturn,
};
use std::sync::{Arc, Mutex};

pub type Manager = InMemoryNodeManager<HistoryManager>;
pub struct HistoryManager {
    namespace: NamespaceMetadata,
    pub samples: Arc<Mutex<Vec<DataValue>>>,
}

pub fn builder(samples: Arc<Mutex<Vec<DataValue>>>) -> impl NodeManagerBuilder {
    InMemoryNodeManagerBuilder::new(move |context: ServerContext, space: &mut AddressSpace| {
        let uri = "urn:hashi:history-fixture";
        let index = context
            .type_tree
            .write()
            .namespaces_mut()
            .add_namespace(uri);
        space.add_namespace(uri, index);
        let value = samples.lock().unwrap().last().cloned().unwrap();
        VariableBuilder::new(
            &NodeId::new(index, "HistoricalValue"),
            opcua::types::QualifiedName::new(index, "HistoricalValue"),
            "Historical value",
        )
        .data_type(DataTypeId::Double)
        .value(value.value.clone().unwrap())
        .historizing(true)
        .access_level(AccessLevel::CURRENT_READ | AccessLevel::HISTORY_READ)
        .user_access_level(AccessLevel::CURRENT_READ | AccessLevel::HISTORY_READ)
        .organized_by(ObjectId::ObjectsFolder)
        .insert(space);
        HistoryManager {
            namespace: NamespaceMetadata {
                namespace_uri: uri.into(),
                namespace_index: index,
                ..Default::default()
            },
            samples,
        }
    })
}

#[async_trait::async_trait]
impl InMemoryNodeManagerImpl for HistoryManager {
    async fn init(&self, _space: &mut AddressSpace, _context: ServerContext) {}
    fn name(&self) -> &str {
        "history fixture"
    }
    fn namespaces(&self) -> Vec<NamespaceMetadata> {
        vec![self.namespace.clone()]
    }
    async fn read_values(
        &self,
        _context: &RequestContext,
        _space: &opcua::sync::RwLock<AddressSpace>,
        nodes: &[&opcua::server::node_manager::ParsedReadValueId],
        _max_age: f64,
        _timestamps: TimestampsToReturn,
    ) -> Vec<DataValue> {
        vec![self.samples.lock().unwrap().last().cloned().unwrap(); nodes.len()]
    }
    async fn history_read_raw_modified(
        &self,
        _context: &RequestContext,
        details: &ReadRawModifiedDetails,
        nodes: &mut [&mut &mut HistoryNode],
        _timestamps: TimestampsToReturn,
    ) -> Result<(), StatusCode> {
        if details.is_read_modified || details.return_bounds {
            return Err(StatusCode::BadHistoryOperationUnsupported);
        }
        for node in nodes {
            let samples = self.samples.lock().unwrap();
            let matching: Vec<_> = samples
                .iter()
                .filter(|sample| {
                    sample.source_timestamp.is_some_and(|time| {
                        time.ticks() >= details.start_time.ticks()
                            && time.ticks() <= details.end_time.ticks()
                    })
                })
                .cloned()
                .collect();
            if matching.is_empty() {
                node.set_status(StatusCode::GoodNoData);
                continue;
            }
            let offset = node
                .continuation_point()
                .and_then(|point| point.get::<usize>())
                .copied()
                .unwrap_or(0);
            let end = matching
                .len()
                .min(offset + details.num_values_per_node as usize);
            node.set_result(HistoryData {
                data_values: Some(matching[offset..end].to_vec()),
            });
            node.set_status(StatusCode::Good);
            if end < matching.len() {
                node.set_next_continuation_point(Some(ContinuationPoint::new(Box::new(end))));
            }
        }
        Ok(())
    }
}
