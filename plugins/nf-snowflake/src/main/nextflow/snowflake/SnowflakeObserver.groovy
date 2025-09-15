package nextflow.snowflake

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j

import nextflow.trace.TraceObserver
import nextflow.trace.TraceRecord
import nextflow.processor.TaskHandler
import nextflow.Session
import java.sql.Connection
import java.sql.Statement
import java.sql.PreparedStatement
import java.sql.SQLException
import java.sql.Timestamp
import java.util.UUID

@Slf4j
@CompileStatic
class SnowflakeObserver implements TraceObserver {

    private String currentRunId

    private Session session

    SnowflakeObserver() {}

    @Override
    void onFlowCreate(Session session) {
        this.session = session
        final Connection connection = SnowflakeConnectionPool.getInstance().getConnection()
        try {
            onFlowCreateImpl(session, connection)
        } finally {
            SnowflakeConnectionPool.getInstance().returnConnection(connection)
        }
    }

    private void onFlowCreateImpl(Session session, Connection connection) {
        boolean tableExists = false
        try {
            final Statement statement = connection.createStatement()
            statement.executeQuery("desc table nxf_execution_history")
            tableExists = true
        } catch (SQLException e) {
            tableExists = false
        }

        if (!tableExists) {
            log.warn "Table nxf_execution_history does not exist, skipping execution history tracking"
            return
        }

        // Generate unique UUID for this run
        currentRunId = UUID.randomUUID().toString()
        
        // Use current time as start time
        final long startTime = System.currentTimeMillis()
        final String insertSql = """
            INSERT INTO nxf_execution_history (run_id, run_name, run_start_time, run_end_time, run_session_id, run_status, submitted_by) 
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """

        final String submittedBy = System.getenv("CURRENT_USER")
        
        try {
            final PreparedStatement preparedStatement = connection.prepareStatement(insertSql)
            preparedStatement.setString(1, currentRunId)
            preparedStatement.setString(2, session.runName)
            preparedStatement.setTimestamp(3, new Timestamp(startTime))
            preparedStatement.setTimestamp(4, null)
            preparedStatement.setString(5, session.uniqueId.toString())
            preparedStatement.setString(6, 'RUNNING')
            preparedStatement.setString(7, submittedBy)
            
            preparedStatement.executeUpdate()
            log.info "Started tracking pipeline execution with run ID: ${currentRunId}"
        } catch (SQLException e) {
            log.warn "Failed to insert execution history record: ${e.message}"
            // Reset currentRunId since tracking failed
            currentRunId = null
        }
    }

    @Override
    void onFlowComplete() {
        if (currentRunId == null) {
            log.warn "No run ID found, skipping flow completion tracking"
            return
        }
        
        final Connection connection = SnowflakeConnectionPool.getInstance().getConnection()
        try {
            onFlowCompleteImpl(connection)
        } finally {
            SnowflakeConnectionPool.getInstance().returnConnection(connection)
        }
    }
    
    private void onFlowCompleteImpl(Connection connection) {
        final long endTime = System.currentTimeMillis()

        final String runStatus = session.isSuccess() ? 'SUCCESS' : 'ERROR'
        final String updateSql = """
            UPDATE nxf_execution_history 
            SET run_end_time = ?, run_status = ? 
            WHERE run_id = ?
        """
        
        try {
            final PreparedStatement preparedStatement = connection.prepareStatement(updateSql)
            preparedStatement.setTimestamp(1, new Timestamp(endTime))
            preparedStatement.setString(2, runStatus)
            preparedStatement.setString(3, currentRunId)
            
            int rowsUpdated = preparedStatement.executeUpdate()
            if (rowsUpdated > 0) {
                log.info "Updated execution history for run ID: ${currentRunId}"
            } else {
                log.warn "No rows updated for run ID: ${currentRunId}"
            }
        } catch (SQLException e) {
            log.warn "Failed to update execution history with completion status: ${e.message}"
        }
    }
}
