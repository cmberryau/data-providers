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

using MySql.Data.MySqlClient;
using OsmSharp.Osm.Data;

namespace OsmSharp.Data.MySQL.Osm
{
    /// <summary>
    /// Tools for creation/detection of the simple schema in MySQL.
    /// </summary>
    public class MySQLSchemaTools : SchemaTools
    {
        #region table commands

        private const string TABLE_DETECT = @"SELECT " + 
                                                @"COUNT(*) " +
                                            @"FROM " +
                                                @"information_schema.tables " +
                                            @"WHERE " +
                                                @"table_schema = '{0}' " +
                                            @"AND " +
                                                @"table_name = '{1}'; ";

        private const string TABLE_DROP = @"DROP TABLE {0}.{1};";

        private const string TABLE_NODE_CREATE = @"CREATE TABLE node " + 
                                                 @"( " +
                                                     @"id BIGINT NOT NULL PRIMARY KEY, " +
                                                     @"latitude INTEGER NULL, " +
                                                     @"longitude INTEGER NULL, " +
                                                     @"changeset_id BIGINT NULL, " +
                                                     @"visible BIT NULL, " +
                                                     @"time_stamp DATETIME NULL, " +
                                                     @"tile BIGINT NULL, " +
                                                     @"version BIGINT UNSIGNED NULL, " +
                                                     @"usr VARCHAR(100) NULL, " +
                                                     @"usr_id BIGINT NULL " +
                                                 @"); ";

        private const string TABLE_NODE_TAGS_CREATE = @"CREATE TABLE node_tags" +
                                                      @"( " +
                                                          @"node_id BIGINT NOT NULL, " +
                                                          @"tag_key VARCHAR(100) NOT NULL, " +
                                                          @"value VARCHAR(500) NULL, " +
                                                          @"PRIMARY KEY (node_id, tag_key) " +
                                                      @"); ";

        private const string TABLE_WAY_CREATE = @"CREATE TABLE way " +
                                                @"( " +
                                                    @"id BIGINT NOT NULL PRIMARY KEY, " +
                                                    @"changeset_id BIGINT NULL, " +
                                                    @"visible BIT NULL, " +
                                                    @"time_stamp DATETIME NULL, " +
                                                    @"version BIGINT UNSIGNED NULL, " +
                                                    @"usr VARCHAR(100) NULL, " +
                                                    @"usr_id BIGINT NULL " +
                                                @"); ";

        private const string TABLE_WAY_TAGS_CREATE = @"CREATE TABLE way_tags" +
                                                     @"( " +
                                                         @"way_id BIGINT NOT NULL, " +
                                                         @"tag_key VARCHAR(100) NOT NULL, " +
                                                         @"value VARCHAR(500) NULL, " +
                                                         @"PRIMARY KEY (way_id, tag_key) " +
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
                                                         @"changeset_id BIGINT NULL, " +
                                                         @"visible BIT NULL, " +
                                                         @"time_stamp DATETIME NULL, " +
                                                         @"version BIGINT UNSIGNED NULL, " +
                                                         @"usr VARCHAR(100) NULL, " +
                                                         @"usr_id BIGINT NULL " +
                                                     @"); ";

        private const string TABLE_RELATION_TAGS_CREATE = @"CREATE TABLE relation_tags" +
                                                          @"( " +
                                                              @"relation_id BIGINT NOT NULL, " +
                                                              @"tag_key VARCHAR(100) NOT NULL, " +
                                                              @"value VARCHAR(500) NULL, " +
                                                              @"PRIMARY KEY (relation_id, tag_key) " +
                                                          @"); ";

        #endregion table commands

        #region index commands

        private const string TABLE_RELATION_MEMBERS_CREATE = @"CREATE TABLE relation_members" +
                                                             @"( " +
                                                                 @"relation_id BIGINT NOT NULL, " +
                                                                 @"member_type TINYINT NOT NULL, " +
                                                                 @"member_id BIGINT NOT NULL, " +
                                                                 @"member_role VARCHAR(100) NULL, " +
                                                                 @"sequence_id BIGINT NOT NULL " +
                                                             @"); ";

        private const string INDEX_DETECT = @"SELECT " +
                                                @"COUNT(1) " +
                                            @"FROM " +
                                                @"information_schema.statistics " +
                                            @"WHERE " +
                                                @"table_schema = '{0}' " +
                                                    @"AND table_name = '{1}' " +
                                                    @"AND index_name = '{2}';";

        private const string INDEX_DROP = @"DROP INDEX `{0}` ON {1}.{2};";

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

        private MySQLSchemaTools()
        {
 
        }

        #region private methods

        /// <summary>
        /// Returns true if the table with the given name exists in the db connected to
        /// </summary>
        /// <param name="connection">The MySQL connection to attempt to detect on</param>
        /// <param name="table_name">The MySQL table to detect</param>
        /// <returns>true if table exists</returns>
        private static bool DetectTable(MySqlConnection connection, string table_name)
        {
            var sql_string = string.Format(TABLE_DETECT, connection.Database, table_name);

            long count = 0;
            using (var command = new MySqlCommand(sql_string, connection))
            {
                count = (long)command.ExecuteScalar();
            }

            return count > 0;
        }

        /// <summary>
        /// Returns true if the index with the given name exists in the db connected to
        /// </summary>
        /// <param name="connection">The MySQL connection to attempt to detect on</param>
        /// <param name="table_name">The MySQL table to detect on</param>
        /// <param name="index_name">The MySQL index to detect</param>
        /// <returns>true if index exists</returns>
        private static bool DetectIndex(MySqlConnection connection, string table_name, string index_name)
        {
            var sql_string = string.Format(INDEX_DETECT, connection.Database, table_name, index_name);

            long count = 0;
            using (var command = new MySqlCommand(sql_string, connection))
            {
                count = (long)command.ExecuteScalar();
            }

            return count > 0;
        }

        /// <summary>
        /// Drops a table from the MySQL db
        /// </summary>
        /// <param name="connection">The MySQL connection to attempt the drop on</param>
        /// <param name="table_name">The MySQL table to drop the index from</param>
        private static void DropTable(MySqlConnection connection, string table_name)
        {
            var sql_string = string.Format(TABLE_DROP, connection.Database, table_name);

            ExecuteScript(connection, sql_string);
        }

        /// <summary>
        /// Drops an index from the MySQL db
        /// </summary>
        /// <param name="connection">The MySQL connection to attempt the drop on</param>
        /// <param name="table_name">The MySQL table to drop</param>
        /// <param name="index_name">The MySQL index to drop</param>
        private static void DropIndex(MySqlConnection connection, string table_name, string index_name)
        {
            var sql_string = string.Format(INDEX_DROP, index_name, connection.Database, table_name);

            ExecuteScript(connection, sql_string);
        }

        /// <summary>
        /// Executes the given script on the database connected to.
        /// </summary>
        /// <param name="connection"></param>
        /// <param name="sql"></param>     
        private static void ExecuteScript(MySqlConnection connection, string sql)
        {
            using (var command = new MySqlCommand(sql, connection))
            {
                command.ExecuteNonQuery();
            }
        }

        private static bool DetectNodeTable(MySqlConnection connection)
        {
            return MySQLSchemaTools.DetectTable(connection, "node");
        }

        private static bool DetectNodeTagsTable(MySqlConnection connection)
        {
            return MySQLSchemaTools.DetectTable(connection, "node_tags");
        }

        private static bool DetectWayTable(MySqlConnection connection)
        {
            return MySQLSchemaTools.DetectTable(connection, "way");
        }

        private static bool DetectWayTagsTable(MySqlConnection connection)
        {
            return MySQLSchemaTools.DetectTable(connection, "way_tags");
        }

        private static bool DetectWayNodesTable(MySqlConnection connection)
        {
            return MySQLSchemaTools.DetectTable(connection, "way_nodes");
        }

        private static bool DetectRelationTable(MySqlConnection connection)
        {
            return MySQLSchemaTools.DetectTable(connection, "relation");
        }

        private static bool DetectRelationTagsTable(MySqlConnection connection)
        {
            return MySQLSchemaTools.DetectTable(connection, "relation_tags");
        }

        private static bool DetectRelationMembersTable(MySqlConnection connection)
        {
            return MySQLSchemaTools.DetectTable(connection, "relation_members");
        }

        private static void CreateNodeTable(MySqlConnection connection)
        {
            MySQLSchemaTools.ExecuteScript(connection, TABLE_NODE_CREATE);
        }

        private static void CreateNodeTagsTable(MySqlConnection connection)
        {
            MySQLSchemaTools.ExecuteScript(connection, TABLE_NODE_TAGS_CREATE);
        }

        private static void CreateWayTable(MySqlConnection connection)
        {
            MySQLSchemaTools.ExecuteScript(connection, TABLE_WAY_CREATE);
        }

        private static void CreateWayTagsTable(MySqlConnection connection)
        {
            MySQLSchemaTools.ExecuteScript(connection, TABLE_WAY_TAGS_CREATE);
        }

        private static void CreateWayNodesTable(MySqlConnection connection)
        {
            MySQLSchemaTools.ExecuteScript(connection, TABLE_WAY_NODES_CREATE);
        }

        private static void CreateRelationTable(MySqlConnection connection)
        {
            MySQLSchemaTools.ExecuteScript(connection, TABLE_RELATION_CREATE);
        }

        private static void CreateRelationTagsTable(MySqlConnection connection)
        {
            MySQLSchemaTools.ExecuteScript(connection, TABLE_RELATION_TAGS_CREATE);
        }

        private static void CreateRelationMembersTable(MySqlConnection connection)
        {
            MySQLSchemaTools.ExecuteScript(connection, TABLE_RELATION_MEMBERS_CREATE);
        }

        private static bool DetectNodeTileIndex(MySqlConnection connection)
        {
            return MySQLSchemaTools.DetectIndex(connection, "node", "IDX_NODE_TILE");
        }

        private static bool DetectWayNodesNodeIndex(MySqlConnection connection)
        {
            return MySQLSchemaTools.DetectIndex(connection, "way_nodes", "IDX_WAY_NODES_NODE");
        }

        private static bool DetectWayNodesWaySequenceIndex(MySqlConnection connection)
        {
            return MySQLSchemaTools.DetectIndex(connection, "way_nodes", "IDX_WAY_NODES_WAY_SEQUENCE");
        }

        private static bool DetectRelationMembersMemberTypeSequenceIndex(MySqlConnection connection)
        {
            return MySQLSchemaTools.DetectIndex(connection, "relation_members", "IDX_RELATION_MEMBERS_MEMBER_TYPE_SEQUENCE");
        }

        private static void CreateNodeTileIndex(MySqlConnection connection)
        {
            MySQLSchemaTools.ExecuteScript(connection, INDEX_NODE_TILE_CREATE);
        }

        private static void CreateWayNodesNodeIndex(MySqlConnection connection)
        {
            MySQLSchemaTools.ExecuteScript(connection, INDEX_WAY_NODES_NODE_CREATE);
        }

        private static void CreateWayNodesWaySequenceIndex(MySqlConnection connection)
        {
            MySQLSchemaTools.ExecuteScript(connection, INDEX_WAY_NODES_WAY_SEQUENCE_CREATE);
        }

        private static void CreateRelationMembersMemberTypeSequenceIndex(MySqlConnection connection)
        {
            MySQLSchemaTools.ExecuteScript(connection, INDEX_RELATION_MEMBERS_MEMBER_TYPE_SEQUENCE_CREATE);
        }

        private static void DetectAndCreateIndexes(MySqlConnection connection)
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
        /// <param name="connection">The MySQLConnection to perform detection and creation on</param>
        public static void CreateAndDetect(MySqlConnection connection)
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
        /// <param name="connection">The MySQLConnection to drop tables from</param>
        public static void Drop(MySqlConnection connection)
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
        }

        #endregion public methods
    }
}
