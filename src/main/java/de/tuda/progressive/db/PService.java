package de.tuda.progressive.db;

import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.metrics.MetricsSystem;
import org.apache.calcite.avatica.remote.LocalService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PService extends LocalService {

	private static final Logger log = LoggerFactory.getLogger(PService.class);

	public PService(Meta meta) {
		super(meta);
	}

	public PService(Meta meta, MetricsSystem metrics) {
		super(meta, metrics);
	}

	@Override
	public void setRpcMetadata(RpcMetadataResponse serverLevelRpcMetadata) {
		log.debug("RpcMetadataResponse: {}", serverLevelRpcMetadata);
		super.setRpcMetadata(serverLevelRpcMetadata);
	}

	@Override
	public ResultSetResponse toResponse(Meta.MetaResultSet resultSet) {
		log.debug("MetaResultSet: {}", resultSet);
		return super.toResponse(resultSet);
	}

	@Override
	public ResultSetResponse apply(CatalogsRequest request) {
		log.debug("CatalogsRequest: {}", request);
		return super.apply(request);
	}

	@Override
	public ResultSetResponse apply(SchemasRequest request) {
		log.debug("SchemasRequest: {}", request);
		return super.apply(request);
	}

	@Override
	public ResultSetResponse apply(TablesRequest request) {
		log.debug("TablesRequest: {}", request);
		return super.apply(request);
	}

	@Override
	public ResultSetResponse apply(TableTypesRequest request) {
		log.debug("TableTypesRequest: {}", request);
		return super.apply(request);
	}

	@Override
	public ResultSetResponse apply(TypeInfoRequest request) {
		log.debug("TypeInfoRequest: {}", request);
		return super.apply(request);
	}

	@Override
	public ResultSetResponse apply(ColumnsRequest request) {
		log.debug("ColumnsRequest: {}", request);
		return super.apply(request);
	}

	@Override
	public PrepareResponse apply(PrepareRequest request) {
		log.debug("PrepareRequest: {}", request);
		return super.apply(request);
	}

	@Override
	public ExecuteResponse apply(PrepareAndExecuteRequest request) {
		log.debug("PrepareAndExecuteRequest: {}", request);
		return super.apply(request);
	}

	@Override
	public FetchResponse apply(FetchRequest request) {
		log.debug("FetchRequest: {}", request);
		return super.apply(request);
	}

	@Override
	public ExecuteResponse apply(ExecuteRequest request) {
		log.debug("ExecuteRequest: {}", request);
		return super.apply(request);
	}

	@Override
	public CreateStatementResponse apply(CreateStatementRequest request) {
		log.debug("CreateStatementRequest: {}", request);
		return super.apply(request);
	}

	@Override
	public CloseStatementResponse apply(CloseStatementRequest request) {
		log.debug("CloseStatementRequest: {}", request);
		return super.apply(request);
	}

	@Override
	public OpenConnectionResponse apply(OpenConnectionRequest request) {
		log.debug("OpenConnectionRequest: {}", request);
		return super.apply(request);
	}

	@Override
	public CloseConnectionResponse apply(CloseConnectionRequest request) {
		log.debug("CloseConnectionRequest: {}", request);
		return super.apply(request);
	}

	@Override
	public ConnectionSyncResponse apply(ConnectionSyncRequest request) {
		log.debug("ConnectionSyncRequest: {}", request);
		return super.apply(request);
	}

	@Override
	public DatabasePropertyResponse apply(DatabasePropertyRequest request) {
		log.debug("DatabasePropertyRequest: {}", request);
		return super.apply(request);
	}

	@Override
	public SyncResultsResponse apply(SyncResultsRequest request) {
		log.debug("SyncResultsRequest: {}", request);
		return super.apply(request);
	}

	@Override
	public CommitResponse apply(CommitRequest request) {
		log.debug("CommitRequest: {}", request);
		return super.apply(request);
	}

	@Override
	public RollbackResponse apply(RollbackRequest request) {
		log.debug("RollbackRequest: {}", request);
		return super.apply(request);
	}

	@Override
	public ExecuteBatchResponse apply(PrepareAndExecuteBatchRequest request) {
		log.debug("PrepareAndExecuteBatchRequest: {}", request);
		return super.apply(request);
	}

	@Override
	public ExecuteBatchResponse apply(ExecuteBatchRequest request) {
		log.debug("ExecuteBatchRequest: {}", request);
		return super.apply(request);
	}
}
