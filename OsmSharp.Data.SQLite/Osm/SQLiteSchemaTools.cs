// OsmSharp - OpenStreetMap (OSM) SDK
// Copyright (C) 2013 Abelshausen Ben
// 
// This file is part of OsmSharp.
// 
// OsmSharp is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 2 of the License, or
// (at your option) any later version.
// 
// OsmSharp is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU General Public License for more details.
// 
// You should have received a copy of the GNU General Public License
// along with OsmSharp. If not, see <http://www.gnu.org/licenses/>.

using OsmSharp.Osm.Data;
using System.Data.SQLite;

namespace OsmSharp.Data.SQLite.Osm
{
    /// <summary>
    /// Tools for creation/detection of the simple schema in SQLite.
    /// </summary>
    public class SQLiteSchemaTools : SchemaTools
    {
        #region constants

        /// <summary>
        /// Default filename for sqlite dbs
        /// </summary>
        private const string DefaultFilePath = "sqlite.db";

        #endregion constants

        #region table commands

        private const string TABLE_DETECT = @"SELECT " +
                                                @"COUNT(1) " +
                                            @"FROM " +
                                                @"sqlite_master " +
                                            @"WHERE " +
                                                @"type = 'table' " +
                                            @"AND " +
                                                @"name = '{0}';";

        private const string TABLE_DROP = @"DROP TABLE {0};";

        private const string TABLE_NODE_CREATE = @"CREATE TABLE node " +
                                                          @"( " +
                                                              @"id BIGINT NOT NULL PRIMARY KEY, " +
                                                              @"latitude INTEGER NULL, " +
                                                              @"longitude INTEGER NULL, " +
                                                              @"tile BIGINT NULL, " +
                                                              @"tags_id INT NULL, " +
                                                              @"name VARCHAR(500) NULL " +
                                                          @"); ";

        private const string TABLE_NODE_TAGS_CREATE = @"CREATE TABLE node_tags" +
                                                             @"( " +
                                                                 @"id INT NOT NULL, " +
                                                                 @"tag_key VARCHAR(100) NOT NULL, " +
                                                                 @"value VARCHAR(500) NULL, " +
                                                                 @"PRIMARY KEY (id, tag_key) " +
                                                             @"); ";

        private const string TABLE_WAY_CREATE = @"CREATE TABLE way " +
                                                         @"( " +
                                                             @"id BIGINT NOT NULL PRIMARY KEY, " +
                                                             @"tags_id INT NULL, " +
                                                             @"name VARCHAR(500) NULL " +
                                                         @"); ";

        private const string TABLE_WAY_TAGS_CREATE = @"CREATE TABLE way_tags" +
                                                             @"( " +
                                                                 @"id INT NOT NULL, " +
                                                                 @"tag_key VARCHAR(100) NOT NULL, " +
                                                                 @"value VARCHAR(500) NULL, " +
                                                                 @"PRIMARY KEY (id, tag_key) " +
                                                             @"); ";

        private const string TABLE_WAY_NODES_CREATE = @"CREATE TABLE way_nodes" +
                                                      @"( " +
                                                          @"way_id BIGINT NOT NULL, " +
                                                          @"node_id BIGINT NOT NULL, " +
                                                          @"sequence_id BIGINT NOT NULL, " +
                                                          @"PRIMARY KEY (way_id, node_id, sequence_id) " +
                                                      @"); ";

        private const string TABLE_RELATION_CREATE = @"CREATE TABLE relation " +
                                                              @"( " +
                                                                  @"id BIGINT NOT NULL PRIMARY KEY, " +
                                                                  @"tags_id INT NULL, " +
                                                                  @"name VARCHAR(500) NULL " +
                                                              @"); ";

        private const string TABLE_RELATION_TAGS_CREATE = @"CREATE TABLE relation_tags" +
                                                         @"( " +
                                                            @"id INT NOT NULL, " +
                                                            @"tag_key VARCHAR(100) NOT NULL, " +
                                                            @"value VARCHAR(500) NULL, " +
                                                            @"PRIMARY KEY (id, tag_key) " +
                                                         @"); ";

        private const string TABLE_RELATION_MEMBERS_CREATE = @"CREATE TABLE relation_members" +
                                                             @"( " +
                                                                 @"relation_id BIGINT NOT NULL, " +
                                                                 @"member_type TINYINT NOT NULL, " +
                                                                 @"member_id BIGINT NOT NULL, " +
                                                                 @"member_role VARCHAR(100) NULL, " +
                                                                 @"sequence_id BIGINT NOT NULL, " +
                                                                 @"PRIMARY KEY (relation_id, member_type, member_id, member_role, sequence_id) " +
                                                             @"); ";

        #endregion table commands

        #region index commands

        private const string INDEX_DETECT = @"SELECT " +
                                                @"COUNT(1) " +
                                            @"FROM " +
                                                @"sqlite_master " +
                                            @"WHERE " +
                                                @"type = 'index' " +
                                            @"AND " +
                                                @"tbl_name = '{0}' " +
                                            @"AND " +
                                                @"name = '{1}';";

        private const string INDEX_DROP = @"DROP INDEX `{0}`;";

        private const string INDEX_NODE_TILE_CREATE = @"CREATE INDEX " +
                                                         @"IDX_NODE_TILE " +
                                                      @"ON " +
                                                        @"node (tile ASC);";

        private const string INDEX_WAY_NODES_NODE_CREATE = @"CREATE INDEX " +
                                                               @"IDX_WAY_NODES_NODE " +
                                                           @"ON " +
                                                               @"way_nodes (node_id ASC);";

        private const string INDEX_WAY_NODES_WAY_SEQUENCE_CREATE = @"CREATE INDEX " +
                                                                       @"IDX_WAY_NODES_WAY_SEQUENCE " +
                                                                   @"ON " +
                                                                       @"way_nodes (way_id ASC, sequence_id ASC);";

        private const string INDEX_RELATION_MEMBERS_MEMBER_TYPE_SEQUENCE_CREATE = @"CREATE INDEX " +
                                                                                      @"IDX_RELATION_MEMBERS_MEMBER_TYPE_SEQUENCE " +
                                                                                  @"ON " +
                                                                                      @"relation_members (member_id ASC, member_type ASC, sequence_id ASC);";

        #endregion index commands

        private const string REMOVE_UNREFERENCED_UNTAGGED_NODES = @"DELETE FROM " +
                                                                        @"node " +
                                                                    @"WHERE " +
                                                                        @"node.id " +
                                                                    @"NOT IN " +
                                                                    @"( " +
                                                                        @"SELECT " +
                                                                            @"way_nodes.node_id " +
                                                                        @"FROM " +
                                                                            @"way_nodes " +
                                                                    @") " +
                                                                    @"AND " +
                                                                        @"node.id " +
                                                                    @"NOT IN " +
                                                                    @"( " +
                                                                        @"SELECT " +
                                                                            @"node.id " +
                                                                        @"FROM " +
                                                                            @"node " +
                                                                        @"WHERE " +
                                                                            @"node.tags_id " +
                                                                        @"IN " +
                                                                        @"( " +
                                                                            @"SELECT " +
                                                                                @"node_tags.id " +
                                                                            @"FROM " +
                                                                                @"node_tags " +
                                                                        @") " +
                                                                    @");";

        private SQLiteSchemaTools() {}

        #region private methods

        /// <summary>
        /// Returns true if the table with the given name exists in the db connected to
        /// </summary>
        /// <param name="connection">The SQLite connection to attempt to detect on</param>
        /// <param name="table_name">The SQLite table to detect</param>
        /// <returns>true if table exists</returns>
        private static bool DetectTable(SQLiteConnection connection, string table_name)
        {
            var sql_string = string.Format(TABLE_DETECT, table_name);

            long count = 0;
            using (var command = new SQLiteCommand(sql_string, connection))
            {
                count = (long)command.ExecuteScalar();
            }

            return count > 0;
        }

        /// <summary>
        /// Returns true if the index with the given name exists in the db connected to
        /// </summary>
        /// <param name="connection">The SQLite connection to attempt to detect on</param>
        /// <param name="table_name">The SQLite table to detect on</param>
        /// <param name="index_name">The SQLite index to detect</param>
        /// <returns>true if index exists</returns>
        private static bool DetectIndex(SQLiteConnection connection, string table_name, string index_name)
        {
            var sql_string = string.Format(INDEX_DETECT, table_name, index_name);

            long count = 0;
            using (var command = new SQLiteCommand(sql_string, connection))
            {
                count = (long)command.ExecuteScalar();
            }

            return count > 0;
        }

        /// <summary>
        /// Drops a table from the SQLite db
        /// </summary>
        /// <param name="connection">The SQLite connection to attempt the drop on</param>
        /// <param name="table_name">The SQLite table to drop the index from</param>
        private static void DropTable(SQLiteConnection connection, string table_name)
        {
            var sql_string = string.Format(TABLE_DROP, table_name);

            ExecuteScript(connection, sql_string);
        }

        /// <summary>
        /// Drops an index from the SQLite db
        /// </summary>
        /// <param name="connection">The SQLite connection to attempt the drop on</param>
        /// <param name="table_name">The SQLite table to drop</param>
        /// <param name="index_name">The SQLite index to drop</param>
        private static void DropIndex(SQLiteConnection connection, string table_name, string index_name)
        {
            var sql_string = string.Format(INDEX_DROP, index_name);

            ExecuteScript(connection, sql_string);
        }

        /// <summary>
        /// Executes the given script on the database connected to.
        /// </summary>
        /// <param name="connection"></param>
        /// <param name="sql"></param>     
        private static void ExecuteScript(SQLiteConnection connection, string sql)
        {
            using (var command = new SQLiteCommand(sql, connection))
            {
                command.ExecuteNonQuery();
            }
        }

        private static bool DetectNodeTable(SQLiteConnection connection)
        {
            return SQLiteSchemaTools.DetectTable(connection, "node");
        }

        private static bool DetectNodeTagsTable(SQLiteConnection connection)
        {
            return SQLiteSchemaTools.DetectTable(connection, "node_tags");
        }

        private static bool DetectWayTable(SQLiteConnection connection)
        {
            return SQLiteSchemaTools.DetectTable(connection, "way");
        }

        private static bool DetectWayTagsTable(SQLiteConnection connection)
        {
            return SQLiteSchemaTools.DetectTable(connection, "way_tags");
        }

        private static bool DetectWayNodesTable(SQLiteConnection connection)
        {
            return SQLiteSchemaTools.DetectTable(connection, "way_nodes");
        }

        private static bool DetectRelationTable(SQLiteConnection connection)
        {
            return SQLiteSchemaTools.DetectTable(connection, "relation");
        }

        private static bool DetectRelationTagsTable(SQLiteConnection connection)
        {
            return SQLiteSchemaTools.DetectTable(connection, "relation_tags");
        }

        private static bool DetectRelationMembersTable(SQLiteConnection connection)
        {
            return SQLiteSchemaTools.DetectTable(connection, "relation_members");
        }

        private static bool DetectUniqueNodeTagsTable(SQLiteConnection connection)
        {
            return SQLiteSchemaTools.DetectTable(connection, "node_tags");
        }

        private static bool DetectUniqueWayTagsTable(SQLiteConnection connection)
        {
            return SQLiteSchemaTools.DetectTable(connection, "way_tags");
        }

        private static bool DetectUniqueRelationTagsTable(SQLiteConnection connection)
        {
            return SQLiteSchemaTools.DetectTable(connection, "relation_tags");
        }

        private static void CreateNodeTable(SQLiteConnection connection)
        {
            SQLiteSchemaTools.ExecuteScript(connection, TABLE_NODE_CREATE);
        }

        private static void CreateNodeTagsTable(SQLiteConnection connection)
        {
            SQLiteSchemaTools.ExecuteScript(connection, TABLE_NODE_TAGS_CREATE);
        }

        private static void CreateWayTable(SQLiteConnection connection)
        {
            SQLiteSchemaTools.ExecuteScript(connection, TABLE_WAY_CREATE);
        }

        private static void CreateWayTagsTable(SQLiteConnection connection)
        {
            SQLiteSchemaTools.ExecuteScript(connection, TABLE_WAY_TAGS_CREATE);
        }

        private static void CreateWayNodesTable(SQLiteConnection connection)
        {
            SQLiteSchemaTools.ExecuteScript(connection, TABLE_WAY_NODES_CREATE);
        }

        private static void CreateRelationTable(SQLiteConnection connection)
        {
            SQLiteSchemaTools.ExecuteScript(connection, TABLE_RELATION_CREATE);
        }

        private static void CreateRelationTagsTable(SQLiteConnection connection)
        {
            SQLiteSchemaTools.ExecuteScript(connection, TABLE_RELATION_TAGS_CREATE);
        }

        private static void CreateRelationMembersTable(SQLiteConnection connection)
        {
            SQLiteSchemaTools.ExecuteScript(connection, TABLE_RELATION_MEMBERS_CREATE);
        }

        private static bool DetectNodeTileIndex(SQLiteConnection connection)
        {
            return SQLiteSchemaTools.DetectIndex(connection, "node", "IDX_NODE_TILE");
        }

        private static bool DetectWayNodesNodeIndex(SQLiteConnection connection)
        {
            return SQLiteSchemaTools.DetectIndex(connection, "way_nodes", "IDX_WAY_NODES_NODE");
        }

        private static bool DetectWayNodesWaySequenceIndex(SQLiteConnection connection)
        {
            return SQLiteSchemaTools.DetectIndex(connection, "way_nodes", "IDX_WAY_NODES_WAY_SEQUENCE");
        }

        private static bool DetectRelationMembersMemberTypeSequenceIndex(SQLiteConnection connection)
        {
            return SQLiteSchemaTools.DetectIndex(connection, "relation_members", "IDX_RELATION_MEMBERS_MEMBER_TYPE_SEQUENCE");
        }

        private static void CreateNodeTileIndex(SQLiteConnection connection)
        {
            SQLiteSchemaTools.ExecuteScript(connection, INDEX_NODE_TILE_CREATE);
        }

        private static void CreateWayNodesNodeIndex(SQLiteConnection connection)
        {
            SQLiteSchemaTools.ExecuteScript(connection, INDEX_WAY_NODES_NODE_CREATE);
        }

        private static void CreateWayNodesWaySequenceIndex(SQLiteConnection connection)
        {
            SQLiteSchemaTools.ExecuteScript(connection, INDEX_WAY_NODES_WAY_SEQUENCE_CREATE);
        }

        private static void CreateRelationMembersMemberTypeSequenceIndex(SQLiteConnection connection)
        {
            SQLiteSchemaTools.ExecuteScript(connection, INDEX_RELATION_MEMBERS_MEMBER_TYPE_SEQUENCE_CREATE);
        }

        private static void DetectAndCreateIndexes(SQLiteConnection connection)
        {
            if (!DetectNodeTileIndex(connection))
            {
                CreateNodeTileIndex(connection);
            }

            if (!DetectWayNodesNodeIndex(connection))
            {
                CreateWayNodesNodeIndex(connection);
            }

            if (!DetectWayNodesWaySequenceIndex(connection))
            {
                CreateWayNodesWaySequenceIndex(connection);
            }

            if (!DetectRelationMembersMemberTypeSequenceIndex(connection))
            {
                CreateRelationMembersMemberTypeSequenceIndex(connection);
            }
        }

        #endregion private methods

        #region public methods

        /// <summary>
        /// Builds a connection string given the parameters
        /// </summary>
        /// <param name="in_memory">Is the DB in-memory?</param>
        /// <param name="path">What is the path of the DB (if any) (ignored for in-memory db's)</param>
        /// <param name="password">What is the password of the DB (if any)</param>
        /// <param name="compressed">Is the DB compressed?</param>
        internal static string BuildConnectionString(bool in_memory, string path = null,
                                                     string password = null, bool compressed = true)
        {
            var connection_string = "FullUri=file:";

            if (in_memory)
            {
                connection_string += ":memory:?cache=shared;";
            }
            else
            {
                if (path == null)
                {
                    connection_string += DefaultFilePath + ";";
                }
                else
                {
                    connection_string += path + ";";
                }
            }

            connection_string += "Version=3;";

            if (password != null)
            {
                connection_string += "Password=" + password + ";";
            }

            if (compressed)
            {
                connection_string += "Compress=True";
            }

            return connection_string;
        }

        /// <summary>
        /// Converts a lon or lat value to a db storable value
        /// </summary>
        public static int GeoToDB(double coord_element)
        {
            return (int)(coord_element * 10000000.0);
        }

        /// <summary>
        /// Converts a db storable value to a lon or lat
        /// </summary>
        public static double DBToGeo(int db_value)
        {
            return (double)(db_value / 10000000.0);
        }

        /// <summary>
        /// Creates the entire schema but also detects existing tables.
        /// </summary>
        /// <param name="connection">The SQLiteConnection to perform detection and creation on</param>
        public static void CreateAndDetect(SQLiteConnection connection)
        {
            if (!DetectNodeTable(connection))
            {
                CreateNodeTable(connection);
            }

            if (!DetectNodeTagsTable(connection))
            {
                CreateNodeTagsTable(connection);
            }

            if (!DetectWayTable(connection))
            {
                CreateWayTable(connection);
            }

            if (!DetectWayTagsTable(connection))
            {
                CreateWayTagsTable(connection);
            }

            if (!DetectWayNodesTable(connection))
            {
                CreateWayNodesTable(connection);
            }

            if (!DetectRelationTable(connection))
            {
                CreateRelationTable(connection);
            }

            if (!DetectRelationTagsTable(connection))
            {
                CreateRelationTagsTable(connection);
            }

            if (!DetectRelationMembersTable(connection))
            {
                CreateRelationMembersTable(connection);
            }

            DetectAndCreateIndexes(connection);
        }

        /// <summary>
        /// Drops the entire schema.
        /// </summary>
        /// <param name="connection">The SQLiteConnection to drop tables from</param>
        public static void Drop(SQLiteConnection connection)
        {
            if (DetectNodeTable(connection))
            {
                DropTable(connection, "node");
            }

            if (DetectNodeTagsTable(connection))
            {
                DropTable(connection, "node_tags");
            }

            if (DetectWayTable(connection))
            {
                DropTable(connection, "way");
            }

            if (DetectWayTagsTable(connection))
            {
                DropTable(connection, "way_tags");
            }

            if (DetectWayNodesTable(connection))
            {
                DropTable(connection, "way_nodes");
            }

            if (DetectRelationTable(connection))
            {
                DropTable(connection, "relation");
            }

            if (DetectRelationTagsTable(connection))
            {
                DropTable(connection, "relation_tags");
            }

            if (DetectRelationMembersTable(connection))
            {
                DropTable(connection, "relation_members");
            }

            if (DetectUniqueNodeTagsTable(connection))
            {
                DropTable(connection, "node_tags");
            }

            if (DetectUniqueWayTagsTable(connection))
            {
                DropTable(connection, "way_tags");
            }

            if (DetectUniqueRelationTagsTable(connection))
            {
                DropTable(connection, "relation_tags");
            }

            // vacuum the db
            using (var command = new SQLiteCommand(connection))
            {
                command.CommandText = "VACUUM;";
                command.ExecuteNonQuery();
            }
        }

        /// <summary>
        /// Post filters the entire DB
        /// </summary>
        public static void PostFilter(SQLiteConnection connection)
        {
            if (DetectNodeTable(connection) && DetectNodeTagsTable(connection))
            {
                ExecuteScript(connection, REMOVE_UNREFERENCED_UNTAGGED_NODES);
            }

            using (var command = new SQLiteCommand(connection))
            {
                command.CommandText = "VACUUM;";
                command.ExecuteNonQuery();
            }
        }

        #endregion public methods
    }
}
