package com.databend.jdbc;

import com.databend.jdbc.cloud.DatabendCopyParams;

import java.io.InputStream;
import java.sql.SQLException;

public interface FileTransferAPI
{
    /**
     * Upload inputStream to the databend internal stage, the data would be uploaded as one file with no split.
     * Caller should close the input stream after the upload is done.
     * @param stageName the stage which receive uploaded file
     * @param destPrefix the prefix of the file name in the stage
     * @param inputStream the input stream of the file
     * @param destFileName the destination file name in the stage
     * @param compressData  whether to compress the data
     * @throws SQLException failed to upload input stream
     */
    public void uploadStream(String stageName, String destPrefix, InputStream inputStream, String destFileName, boolean compressData) throws SQLException;

    /**
     * Download a file from the databend internal stage, the data would be downloaded as one file with no split.
     * @param stageName the stage which contains the file
     * @param sourceFileName the file name in the stage
     * @param decompress whether to decompress the data
     * @return the input stream of the file
     * @throws SQLException
     */
    public InputStream downloadStream(String stageName, String sourceFileName, boolean decompress) throws SQLException;

    /**
     * Copy into the target table from files on the internal stage
     * Documentation: https://databend.rs/doc/sql-commands/dml/dml-copy-into-table
     *
     * @param database the target table's database
     * @param tableName the target table name
     * @param params copy options and file options
     * @throws SQLException
     */
    public void copyIntoTable(String database, String tableName,  DatabendCopyParams params) throws SQLException;
}
