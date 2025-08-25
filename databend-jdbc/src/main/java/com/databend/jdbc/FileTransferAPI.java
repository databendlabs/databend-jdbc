package com.databend.jdbc;

import com.databend.jdbc.cloud.DatabendCopyParams;

import java.io.InputStream;
import java.sql.SQLException;

public interface FileTransferAPI {
    /**
     * Upload inputStream to the databend internal stage, the data would be uploaded as one file with no split.
     * Caller should close the input stream after the upload is done.
     *
     * @param stageName the stage which receive uploaded file
     * @param destPrefix the prefix of the file name in the stage
     * @param inputStream the input stream of the file
     * @param destFileName the destination file name in the stage
     * @param fileSize the file size in the stage
     * @param compressData whether to compress the data
     * @throws SQLException failed to upload input stream
     */
    void uploadStream(String stageName, String destPrefix, InputStream inputStream, String destFileName, long fileSize, boolean compressData) throws SQLException;

    /**
     * Download a file from the databend internal stage, the data would be downloaded as one file with no split.
     *
     * @param stageName the stage which contains the file
     * @param sourceFileName the file name in the stage
     * @param decompress whether to decompress the data
     * @return the input stream of the file
     * @throws SQLException failed to download input stream
     */
    InputStream downloadStream(String stageName, String sourceFileName, boolean decompress) throws SQLException;

    /**
     * Copy into the target table from files on the internal stage
     * Documentation: <a href="https://databend.rs/doc/sql-commands/dml/dml-copy-into-table">...</a>
     *
     * @param database the target table's database
     * @param tableName the target table name
     * @param params copy options and file options
     * @throws SQLException fail to copy into table
     */
    void copyIntoTable(String database, String tableName, DatabendCopyParams params) throws SQLException;

    /**
     * Upload inputStream into the target table
     *
     * @param sql the sql with syntax `Insert into <table> [(<columns></columns>) [values (?, ...)]] from @_databend_load [file_format=(...)]`
     * @param inputStream the input stream of the file
     * @param loadMethod one of "stage" or "streaming"
     * @return num of rows loaded
     * @throws SQLException fail to load file into table
     */
    int loadStreamToTable(String sql, InputStream inputStream, long fileSize, String loadMethod) throws SQLException;
}
