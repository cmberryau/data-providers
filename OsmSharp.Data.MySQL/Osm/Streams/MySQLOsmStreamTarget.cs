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

using System;
using OsmSharp.Osm.Streams;
using MySql.Data.MySqlClient;
using OsmSharp.Osm;
using OsmSharp.Collections.Tags;
using System.Collections.Generic;
using System.Data;
using OsmSharp.Osm.Tiles;

namespace OsmSharp.Data.MySQL.Osm.Streams
{
    /// <summary>
    /// A data processor target for the MySQL schema.
    /// </summary>
    public class MySQLOsmStreamTarget : OsmStreamTarget
    {
        /// <summary>
        /// Does this object own it's own connection?
        /// </summary>
        public bool OwnsConnection
        {
            get
            {
                return _connection_owner;
            }

            set
            {
                _connection_owner = value;
            }
        }

        /// <summary>
        /// Indicates if the MySQL target supports concurrent copies
        /// </summary>
        public override bool SupportsConcurrentCopies
        {
            get
            {
                return true;
            }
        }

        /// <summary>
        /// Holds the connection string.
        /// </summary>
        private string _connection_string;

        /// <summary>
        /// Was the connection created by this object?
        /// </summary>
        private bool _connection_owner;

        /// <summary>
        /// Holds the connection.
        /// </summary>
        private MySqlConnection _connection;

        /// <summary>
        /// Flag that indicates if the schema needs to be created if not present.
        /// </summary>
        private bool _create_and_detect_schema;

        #region command strings

        /// <summary>
        /// MySQL command string to prefix a batch node insertion
        /// </summary>
        private const string REPLACE_NODE_BATCH_PREFIX = @"REPLACE INTO node " +
                                                         @"( " +
                                                             @"id, " +
                                                             @"latitude, " +
                                                             @"longitude, " +
                                                             @"changeset_id, " +
                                                             @"visible, " +
                                                             @"time_stamp, " +
                                                             @"tile, " +
                                                             @"version, " +
                                                             @"usr, " +
                                                             @"usr_id " +
                                                         @") " +
                                                         @"VALUES ";

        /// <summary>
        /// MySQL command string to be attached to a batch node insertion prefix
        /// </summary>
        private const string REPLACE_NODE_BATCH_VALUES = @"( " +
                                                             @"@id{0}, " +
                                                             @"@latitude{0}, " +
                                                             @"@longitude{0}, " +
                                                             @"@changeset_id{0}, " +
                                                             @"@visible{0}, " +
                                                             @"@time_stamp{0}, " +
                                                             @"@tile{0}, " +
                                                             @"@version{0}, " +
                                                             @"@usr{0}, " +
                                                             @"@usr_id{0} " +
                                                         @")";

        /// <summary>
        /// MySQL command string to insert or replace a single node
        /// </summary>
        private const string REPLACE_NODE = REPLACE_NODE_BATCH_PREFIX + @"( " +
                                                                            @"@id, " +
                                                                            @"@latitude, " +
                                                                            @"@longitude, " +
                                                                            @"@changeset_id, " +
                                                                            @"@visible, " +
                                                                            @"@time_stamp, " +
                                                                            @"@tile, " +
                                                                            @"@version, " +
                                                                            @"@usr, " +
                                                                            @"@usr_id " +
                                                                        @")";


        /// <summary>
        /// MySQL command string to prefix a batch insert or replace of node tags
        /// </summary>
        private const string REPLACE_NODE_TAGS_BATCH_PREFIX = @"REPLACE INTO node_tags " +
                                                              @"( " +
                                                                  @"node_id, " +
                                                                  @"tag_key, " +
                                                                  @"value " +
                                                              @") " + 
                                                              @"VALUES ";

        /// <summary>
        /// MySQL command string to be attached to a batch insertion or replace of node tags
        /// </summary>
        private const string REPLACE_NODE_TAGS_BATCH_VALUES = @"( " +
                                                                  @"@node_id{0}, " +
                                                                  @"@tag_key{0}, " +
                                                                  @"@value{0} " +
                                                              @")";

        /// <summary>
        /// MySQL command string to insert or replace a single node tag
        /// </summary>
        private const string REPLACE_NODE_TAGS = REPLACE_NODE_TAGS_BATCH_PREFIX + @"( " +
                                                                                      @"@node_id, " +
                                                                                      @"@tag_key, " +
                                                                                      @"@value " +
                                                                                  @")";


        /// <summary>
        /// MySQL command string to prefix a batch way insertion
        /// </summary>
        private const string REPLACE_WAY_BATCH_PREFIX = @"REPLACE INTO way " +
                                                        @"( " +
                                                            @"id, " +
                                                            @"changeset_id, " +
                                                            @"visible, " +
                                                            @"time_stamp, " +
                                                            @"version, " +
                                                            @"usr, " +
                                                            @"usr_id " +
                                                        @") " +
                                                        @"VALUES ";

        /// <summary>
        /// MySQL command string to be attached to a batch way insertion prefix
        /// </summary>
        private const string REPLACE_WAY_BATCH_VALUES = @"( " +
                                                            @"@id{0}, " +
                                                            @"@changeset_id{0}, " +
                                                            @"@visible{0}, " +
                                                            @"@time_stamp{0}, " +
                                                            @"@version{0}, " +
                                                            @"@usr{0}, " +
                                                            @"@usr_id{0} " +
                                                        @")";

        /// <summary>
        /// MySQL command string to insert or replace a single way
        /// </summary>
        private const string REPLACE_WAY = REPLACE_WAY_BATCH_PREFIX + @"( " +
                                                                          @"@id, " +
                                                                          @"@changeset_id, " +
                                                                          @"@visible, " +
                                                                          @"@time_stamp, " +
                                                                          @"@version, " +
                                                                          @"@usr, " +
                                                                          @"@usr_id " +
                                                                      @")";

        /// <summary>
        /// MySQL command string to prefix a batch insert or replace of way tags
        /// </summary>
        private const string REPLACE_WAY_TAGS_BATCH_PREFIX = @"REPLACE INTO way_tags " +
                                                              @"( " +
                                                                  @"way_id, " +
                                                                  @"tag_key, " +
                                                                  @"value " +
                                                              @") " +
                                                              @"VALUES ";

        /// <summary>
        /// MySQL command string to be attached to a batch insertion or replace of way tags
        /// </summary>
        private const string REPLACE_WAY_TAGS_BATCH_VALUES = @"( " +
                                                                  @"@way_id{0}, " +
                                                                  @"@tag_key{0}, " +
                                                                  @"@value{0} " +
                                                              @")";

        /// <summary>
        /// MySQL command string to insert or replace a single way tag
        /// </summary>
        private const string REPLACE_WAY_TAGS = REPLACE_WAY_TAGS_BATCH_PREFIX + @"( " +
                                                                                      @"@way_id, " +
                                                                                      @"@tag_key, " +
                                                                                      @"@value " +
                                                                                  @")";

        /// <summary>
        /// MySQL command string to prefix a batch insert or replace of way nodes
        /// </summary>
        private const string REPLACE_WAY_NODES_BATCH_PREFIX = @"REPLACE INTO way_nodes " +
                                                              @"( " +
                                                                  @"way_id, " +
                                                                  @"node_id, " +
                                                                  @"sequence_id " +
                                                              @") " +
                                                              @"VALUES ";

        /// <summary>
        /// MySQL command string to be attached to a batch insertion or replace of way nodes
        /// </summary>
        private const string REPLACE_WAY_NODES_BATCH_VALUES = @"( " +
                                                                  @"@way_id{0}, " +
                                                                  @"@node_id{0}, " +
                                                                  @"@sequence_id{0} " +
                                                              @")";

        /// <summary>
        /// MySQL command string to insert or replace a single way node
        /// </summary>
        private const string REPLACE_WAY_NODES = REPLACE_WAY_NODES_BATCH_PREFIX + @"( " +
                                                                                      @"@way_id, " +
                                                                                      @"@node_id, " +
                                                                                      @"@sequence_id " +
                                                                                  @")";

        /// <summary>
        /// MySQL command string to prefix a batch relation insertion
        /// </summary>
        private const string REPLACE_RELATION_BATCH_PREFIX = @"REPLACE INTO relation " +
                                                             @"( " +
                                                                 @"id, " +
                                                                 @"changeset_id, " +
                                                                 @"visible, " +
                                                                 @"time_stamp, " +
                                                                 @"version, " +
                                                                 @"usr, " +
                                                                 @"usr_id " +
                                                             @") " +
                                                             @"VALUES ";

        /// <summary>
        /// MySQL command string to be attached to a batch relation insertion prefix
        /// </summary>
        private const string REPLACE_RELATION_BATCH_VALUES = @"( " +
                                                                 @"@id{0}, " +
                                                                 @"@changeset_id{0}, " +
                                                                 @"@visible{0}, " +
                                                                 @"@time_stamp{0}, " +
                                                                 @"@version{0}, " +
                                                                 @"@usr{0}, " +
                                                                 @"@usr_id{0} " +
                                                             @")";

        /// <summary>
        /// MySQL command string to insert or replace a single relation
        /// </summary>
        private const string REPLACE_RELATION = REPLACE_RELATION_BATCH_PREFIX + @"( " +
                                                                                    @"@id, " +
                                                                                    @"@changeset_id, " +
                                                                                    @"@visible, " +
                                                                                    @"@time_stamp, " +
                                                                                    @"@version, " +
                                                                                    @"@usr, " +
                                                                                    @"@usr_id " +
                                                                                @")";

        /// <summary>
        /// MySQL command string to prefix a batch insert or replace of relation tags
        /// </summary>
        private const string REPLACE_RELATION_TAGS_BATCH_PREFIX = @"REPLACE INTO relation_tags " +
                                                              @"( " +
                                                                  @"relation_id, " +
                                                                  @"tag_key, " +
                                                                  @"value " +
                                                              @") " +
                                                              @"VALUES ";

        /// <summary>
        /// MySQL command string to be attached to a batch insertion or replace of relation tags
        /// </summary>
        private const string REPLACE_RELATION_TAGS_BATCH_VALUES = @"( " +
                                                                  @"@relation_id{0}, " +
                                                                  @"@tag_key{0}, " +
                                                                  @"@value{0} " +
                                                              @")";

        /// <summary>
        /// MySQL command string to insert or replace a single relation tag
        /// </summary>
        private const string REPLACE_RELATION_TAGS = REPLACE_RELATION_TAGS_BATCH_PREFIX + @"( " +
                                                                                      @"@relation_id, " +
                                                                                      @"@tag_key, " +
                                                                                      @"@value " +
                                                                                  @")";

        /// <summary>
        /// MySQL command string to prefix a batch insert or replace of relation members
        /// </summary>
        private const string REPLACE_RELATION_MEMBERS_BATCH_PREFIX = @"REPLACE INTO relation_members " +
                                                                     @"( " +
                                                                         @"relation_id, " +
                                                                         @"member_type, " +
                                                                         @"member_id, " +
                                                                         @"member_role, " +
                                                                         @"sequence_id " +
                                                                     @") " +
                                                                     @"VALUES ";

        /// <summary>
        /// MySQL command string to be attached to a batch insertion or replace of relation members
        /// </summary>
        private const string REPLACE_RELATION_MEMBERS_BATCH_VALUES = @"( " +
                                                                         @"@relation_id{0}, " +
                                                                         @"@member_type{0}, " +
                                                                         @"@member_id{0}, " +
                                                                         @"@member_role{0}, " +
                                                                         @"@sequence_id{0} " +
                                                                     @")";

        /// <summary>
        /// MySQL command string to insert or replace a single relation member
        /// </summary>
        private const string REPLACE_RELATION_MEMBERS = REPLACE_RELATION_MEMBERS_BATCH_PREFIX + @"( " +
                                                                                                    @"@relation_id, " +
                                                                                                    @"@member_type, " +
                                                                                                    @"@member_id, " +
                                                                                                    @"@member_role, " +
                                                                                                    @"@sequence_id " +
                                                                                                @")";

        #endregion command strings

        #region command objects

        /// <summary>
        /// MySQL command to insert or replace nodes in batch mode
        /// </summary>
        private MySqlCommand _replaceNodeBatchCommand;

        /// <summary>
        /// MySQL command to insert or replace a node in flush mode
        /// </summary>
        private MySqlCommand _replaceNodeFlushCommand;

        /// <summary>
        /// MySQL command to insert or replace node tags in batch mode
        /// </summary>
        private MySqlCommand _replaceNodeTagsBatchCommand;

        /// <summary>
        /// MySQL command to insert or replace a node tag in flush mode
        /// </summary>
        private MySqlCommand _replaceNodeTagFlushCommand;

        /// <summary>
        /// MySQL command to insert or replace a way in batch mode
        /// </summary>
        private MySqlCommand _replaceWayBatchCommand;

        /// <summary>
        /// MySQL command to insert or replace a way in flush mode
        /// </summary>
        private MySqlCommand _replaceWayFlushCommand;

        /// <summary>
        /// MySQL command to insert or replace way tags in batch mode
        /// </summary>
        private MySqlCommand _replaceWayTagsBatchCommand;

        /// <summary>
        /// MySQL command to insert or replace a way tag in flush mode
        /// </summary>
        private MySqlCommand _replaceWayTagFlushCommand;

        /// <summary>
        /// MySQL command to insert or replace way nodes in batch mode
        /// </summary>
        private MySqlCommand _replaceWayNodesBatchCommand;

        /// <summary>
        /// MySQL command to insert or replace a way node in flush mode
        /// </summary>
        private MySqlCommand _replaceWayNodeFlushCommand;

        /// <summary>
        /// MySQL command to insert or replace relations in batch mode
        /// </summary>
        private MySqlCommand _replaceRelationsBatchCommand;

        /// <summary>
        /// MySQL command to insert or replace a relation in flush mode
        /// </summary>
        private MySqlCommand _replaceRelationFlushCommand;

        /// <summary>
        /// MySQL command to insert or replace relation tags in batch mode
        /// </summary>
        private MySqlCommand _replaceRelationTagsBatchCommand;

        /// <summary>
        /// MySQL command to insert or replace a relation tag in flush mode
        /// </summary>
        private MySqlCommand _replaceRelationTagFlushCommand;

        /// <summary>
        /// MySQL command to insert or replace relation members in batch mode
        /// </summary>
        private MySqlCommand _replaceRelationMembersBatchCommand;

        /// <summary>
        /// MySQL command to insert or replace a relation member in flush mode
        /// </summary>
        private MySqlCommand _replaceRelationMemberFlushCommand;

        #endregion command objects

        #region batch fields

        /// <summary>
        /// The number of nodes to process during a batch push to the db
        /// </summary>
        private const int BATCH_NODES_COUNT = 256;

        /// <summary>
        /// The collection of cached nodes
        /// </summary>
        private List<Node> _cached_nodes;

        /// <summary>
        /// The total number of nodes pushed to the db
        /// </summary>
        private long _total_node_count = 0;

        /// <summary>
        /// The number of node tags to process during a batch push to the db
        /// </summary>
        private const int BATCH_NODE_TAGS_COUNT = 256;

        /// <summary>
        /// The collection of cached node tags
        /// </summary>
        private List<Tuple<long?, Tag>> _cached_node_tags;

        /// <summary>
        /// The total number of node tags pushed to the db
        /// </summary>
        private long _total_node_tags_count = 0;

        /// <summary>
        /// The number of ways to process during a batch push to the db
        /// </summary>
        private const int BATCH_WAYS_COUNT = 256;

        /// <summary>
        /// The collection of cached ways
        /// </summary>
        private List<Way> _cached_ways;

        /// <summary>
        /// The total number of ways pushed to the db
        /// </summary>
        private long _total_way_count = 0;

        /// <summary>
        /// The number of way tags to process during a batch push to the db
        /// </summary>
        private const int BATCH_WAY_TAGS_COUNT = 256;

        /// <summary>
        /// The collection of cached way tags
        /// </summary>
        private List<Tuple<long?, Tag>> _cached_way_tags;

        /// <summary>
        /// The total number of way tags pushed to the db
        /// </summary>
        private long _total_way_tags_count = 0;

        /// <summary>
        /// The number of way nodes to process during a batch push to the db
        /// </summary>
        private const int BATCH_WAY_NODES_COUNT = 256;

        /// <summary>
        /// The collection of cached way nodes
        /// </summary>
        private List<Tuple<long?, long?, long?>> _cached_way_nodes;

        /// <summary>
        /// The total number of way nodes pushed to the db
        /// </summary>
        private long _total_way_nodes_count = 0;

        /// <summary>
        /// The number of relations to process during a batch push to the db
        /// </summary>
        private const int BATCH_RELATIONS_COUNT = 256;

        /// <summary>
        /// The collection of cached relations
        /// </summary>
        private List<Relation> _cached_relations;

        /// <summary>
        /// The total number of relations pushed to the db
        /// </summary>
        private long _total_relation_count = 0;

        /// <summary>
        /// The number of relation tags to process during a batch push to the db
        /// </summary>
        private const int BATCH_RELATION_TAGS_COUNT = 256;

        /// <summary>
        /// The collection of cached relation tags
        /// </summary>
        private List<Tuple<long?, Tag>> _cached_relation_tags;

        /// <summary>
        /// The total number of relation tags pushed to the db
        /// </summary>
        private long _total_relation_tags_count = 0;

        /// <summary>
        /// The number of relation members to process during a batch push to the db
        /// </summary>
        private const int BATCH_RELATION_MEMBERS_COUNT = 256;

        /// <summary>
        /// The collection of cached relation members
        /// </summary>
        private List<Tuple<long?, RelationMember, long?>> _cached_relation_members;

        /// <summary>
        /// The total number of relation members pushed to the db
        /// </summary>
        private long _total_relation_members_count = 0;

        #endregion batch fields

        /// <summary>
        /// Creates a new MySQL target
        /// </summary>
        /// <param name="connection">The MySQL connection to use</param>
        /// <param name="password">The password used to connect to the MySQL data base</param>
        /// <param name="create_schema">Should we detect if the db tables exist and attempt to create them?</param>
        public MySQLOsmStreamTarget(MySqlConnection connection, string password, bool create_schema = false)
        {
            _connection = connection;
            _connection_string = connection.ConnectionString;

            if (!_connection_string.Contains("Password") && password == null)
            {
                throw new ArgumentNullException("Password must be provided for the MySQL connection");
            }

            if (!_connection_string.Contains("Password"))
            {
                _connection_string += @";Password=" + password + ";";
            }

            _create_and_detect_schema = create_schema;
            _connection_owner = false;
        }

        /// <summary>
        /// Creates a new MySQL target
        /// </summary>
        /// <param name="connection_string">The MySQL connection string to use</param>
        /// <param name="create_schema">Should we detect if the db tables exist and attempt to create them?</param>
        public MySQLOsmStreamTarget(string connection_string, bool create_schema = false)
        {
            _connection_string = connection_string;
            _create_and_detect_schema = create_schema;
            _connection_owner = false;
        }

        /// <summary>
        /// Creates a new MySQL target
        /// </summary>
        /// <param name="hostname">The hostname of the MySQL server</param>
        /// <param name="database">The MySQL schema name</param>
        /// <param name="username">The username to connect with, sent over plain text</param>
        /// <param name="password">The password to connect with, sent over plain text</param>
        /// <param name="create_schema">Should we detect if the db tables exist and attempt to create them?</param>
        public MySQLOsmStreamTarget(string hostname,
                                    string database,
                                    string username,
                                    string password,
                                    bool create_schema = false)
        {
            _connection_string = @"Server=" + hostname + @";" +
                                @"Database=" + database + @";" +
                                @"User ID=" + username + @";" +
                                @"Password=" + password + @";" +
                                @"Pooling=" + @"false" + @";";

            _create_and_detect_schema = create_schema;
            _connection_owner = false;
        }

        /// <summary>
        /// Initializes this target.
        /// </summary>
        public override void Initialize()
        {
            if (_connection == null)
            {
                _connection = new MySqlConnection(_connection_string);
                _connection_owner = true;
            }
            if (_connection.State != System.Data.ConnectionState.Open)
            {
                _connection.Open();
            }

            if (_create_and_detect_schema)
            { // creates or detects the tables
                MySQLSchemaTools.CreateAndDetect(_connection);
            }

            InitializeCacheAndCommands(_connection);
        }

        #region initialization

        /// <summary>
        /// Initializes all MySQL commands and caches
        /// </summary>
        /// <param name="connection">The MySQL connection to use</param>
        private void InitializeCacheAndCommands(MySqlConnection connection)
        {
            InitializeReplaceNodeCacheAndCommand(connection);
            InitializeReplaceNodeTagCacheAndCommand(connection);
            InitializeReplaceWayCacheAndCommand(connection);
            InitializeReplaceWayTagsCacheAndCommand(connection);
            InitializeReplaceWayNodesCacheAndCommand(connection);
            InitializeReplaceRelationCacheAndCommand(connection);
            InitializeReplaceRelationTagsCacheAndCommand(connection);
            InitializeReplaceRelationMembersCacheAndCommand(connection);
        }

        /// <summary>
        /// Initializes the replace node command and cache
        /// </summary>
        /// <param name="connection">The MySQL connection to use</param>
        private void InitializeReplaceNodeCacheAndCommand(MySqlConnection connection)
        {
            var sql_string = REPLACE_NODE_BATCH_PREFIX;

            for (int i = 0; i < BATCH_NODES_COUNT; ++i)
            {
                sql_string += String.Format(REPLACE_NODE_BATCH_VALUES, i);

                if (i != BATCH_NODES_COUNT - 1)
                {
                    sql_string += @",";
                }
            }

            sql_string += @";";

            _replaceNodeBatchCommand = new MySqlCommand(sql_string, connection);

            for (int i = 0; i < BATCH_NODES_COUNT; ++i)
            {
                _replaceNodeBatchCommand.Parameters.Add(@"@id" + i, MySqlDbType.Int64);
                _replaceNodeBatchCommand.Parameters.Add(@"@latitude" + i, MySqlDbType.Int32);
                _replaceNodeBatchCommand.Parameters.Add(@"@longitude" + i, MySqlDbType.Int32);
                _replaceNodeBatchCommand.Parameters.Add(@"@changeset_id" + i, MySqlDbType.Int64);
                _replaceNodeBatchCommand.Parameters.Add(@"@visible" + i, MySqlDbType.Bit);
                _replaceNodeBatchCommand.Parameters.Add(@"@time_stamp" + i, MySqlDbType.DateTime);
                _replaceNodeBatchCommand.Parameters.Add(@"@tile" + i, MySqlDbType.Int64);
                _replaceNodeBatchCommand.Parameters.Add(@"@version" + i, MySqlDbType.Int64);
                _replaceNodeBatchCommand.Parameters.Add(@"@usr" + i, MySqlDbType.String);
                _replaceNodeBatchCommand.Parameters.Add(@"@usr_id" + i, MySqlDbType.Int32);
            }

            sql_string = REPLACE_NODE;

            _replaceNodeFlushCommand = new MySqlCommand(sql_string, connection);

            _replaceNodeFlushCommand.Parameters.Add(@"@id", MySqlDbType.Int64);
            _replaceNodeFlushCommand.Parameters.Add(@"@latitude", MySqlDbType.Int32);
            _replaceNodeFlushCommand.Parameters.Add(@"@longitude", MySqlDbType.Int32);
            _replaceNodeFlushCommand.Parameters.Add(@"@changeset_id", MySqlDbType.Int64);
            _replaceNodeFlushCommand.Parameters.Add(@"@visible", MySqlDbType.Bit);
            _replaceNodeFlushCommand.Parameters.Add(@"@time_stamp", MySqlDbType.DateTime);
            _replaceNodeFlushCommand.Parameters.Add(@"@tile", MySqlDbType.Int64);
            _replaceNodeFlushCommand.Parameters.Add(@"@version", MySqlDbType.Int64);
            _replaceNodeFlushCommand.Parameters.Add(@"@usr", MySqlDbType.String);
            _replaceNodeFlushCommand.Parameters.Add(@"@usr_id", MySqlDbType.Int32);

            _cached_nodes = new List<Node>();
        }

        /// <summary>
        /// Initializes the replace node tag command and cache
        /// </summary>
        /// <param name="connection">The MySQL connection to use</param>
        private void InitializeReplaceNodeTagCacheAndCommand(MySqlConnection connection)
        {
            var sql_string = REPLACE_NODE_TAGS_BATCH_PREFIX;

            for (int i = 0; i < BATCH_NODE_TAGS_COUNT; ++i)
            {
                sql_string += String.Format(REPLACE_NODE_TAGS_BATCH_VALUES, i);

                if (i != BATCH_NODE_TAGS_COUNT - 1)
                {
                    sql_string += @",";
                }
            }

            sql_string += @";";

            _replaceNodeTagsBatchCommand = new MySqlCommand(sql_string, connection);

            for (int i = 0; i < BATCH_NODE_TAGS_COUNT; ++i)
            {
                _replaceNodeTagsBatchCommand.Parameters.Add(@"node_id" + i, MySqlDbType.Int64);
                _replaceNodeTagsBatchCommand.Parameters.Add(@"tag_key" + i, MySqlDbType.String);
                _replaceNodeTagsBatchCommand.Parameters.Add(@"value" + i, MySqlDbType.String); 
            }

            sql_string = REPLACE_NODE_TAGS;

            _replaceNodeTagFlushCommand = new MySqlCommand(sql_string, connection);

            _replaceNodeTagFlushCommand.Parameters.Add(@"node_id", MySqlDbType.Int64);
            _replaceNodeTagFlushCommand.Parameters.Add(@"tag_key", MySqlDbType.String);
            _replaceNodeTagFlushCommand.Parameters.Add(@"value", MySqlDbType.String); 

            _cached_node_tags = new List<Tuple<long?, Tag>>();
        }

        /// <summary>
        /// Initializes the replace way command and cache
        /// </summary>
        /// <param name="connection">The MySQL connection to use</param>
        private void InitializeReplaceWayCacheAndCommand(MySqlConnection connection)
        {
            var sql_string = REPLACE_WAY_BATCH_PREFIX;

            for (int i = 0; i < BATCH_WAYS_COUNT; ++i)
            {
                sql_string += String.Format(REPLACE_WAY_BATCH_VALUES, i);

                if (i != BATCH_WAYS_COUNT - 1)
                {
                    sql_string += @",";
                }
            }

            sql_string += @";";

            _replaceWayBatchCommand = new MySqlCommand(sql_string, connection);

            for (int i = 0; i < BATCH_WAYS_COUNT; ++i)
            {
                _replaceWayBatchCommand.Parameters.Add(@"@id" + i, MySqlDbType.Int64);
                _replaceWayBatchCommand.Parameters.Add(@"@changeset_id" + i, MySqlDbType.Int64);
                _replaceWayBatchCommand.Parameters.Add(@"@visible" + i, MySqlDbType.Bit);
                _replaceWayBatchCommand.Parameters.Add(@"@time_stamp" + i, MySqlDbType.DateTime);
                _replaceWayBatchCommand.Parameters.Add(@"@version" + i, MySqlDbType.Int64);
                _replaceWayBatchCommand.Parameters.Add(@"@usr" + i, MySqlDbType.String);
                _replaceWayBatchCommand.Parameters.Add(@"@usr_id" + i, MySqlDbType.Int32);
            }

            sql_string = REPLACE_WAY;

            _replaceWayFlushCommand = new MySqlCommand(sql_string, connection);

            _replaceWayFlushCommand.Parameters.Add(@"@id", MySqlDbType.Int64);
            _replaceWayFlushCommand.Parameters.Add(@"@changeset_id", MySqlDbType.Int64);
            _replaceWayFlushCommand.Parameters.Add(@"@visible", MySqlDbType.Bit);
            _replaceWayFlushCommand.Parameters.Add(@"@time_stamp", MySqlDbType.DateTime);
            _replaceWayFlushCommand.Parameters.Add(@"@version", MySqlDbType.Int64);
            _replaceWayFlushCommand.Parameters.Add(@"@usr", MySqlDbType.String);
            _replaceWayFlushCommand.Parameters.Add(@"@usr_id", MySqlDbType.Int32);

            _cached_ways = new List<Way>();
        }

        /// <summary>
        /// Initializes the replace way tags command and cache
        /// </summary>
        /// <param name="connection">The MySQL connection to use</param>
        private void InitializeReplaceWayTagsCacheAndCommand(MySqlConnection connection)
        {
            var sql_string = REPLACE_WAY_TAGS_BATCH_PREFIX;

            for (int i = 0; i < BATCH_WAY_TAGS_COUNT; ++i)
            {
                sql_string += String.Format(REPLACE_WAY_TAGS_BATCH_VALUES, i);

                if (i != BATCH_WAY_TAGS_COUNT - 1)
                {
                    sql_string += @",";
                }
            }

            sql_string += @";";

            _replaceWayTagsBatchCommand = new MySqlCommand(sql_string, connection);

            for (int i = 0; i < BATCH_WAYS_COUNT; ++i)
            {
                _replaceWayTagsBatchCommand.Parameters.Add(@"way_id" + i, MySqlDbType.Int64);
                _replaceWayTagsBatchCommand.Parameters.Add(@"tag_key" + i, MySqlDbType.String);
                _replaceWayTagsBatchCommand.Parameters.Add(@"value" + i, MySqlDbType.String); 
            }

            sql_string = REPLACE_WAY_TAGS;

            _replaceWayTagFlushCommand = new MySqlCommand(sql_string, connection);

            _replaceWayTagFlushCommand.Parameters.Add(@"way_id", MySqlDbType.Int64);
            _replaceWayTagFlushCommand.Parameters.Add(@"tag_key", MySqlDbType.String);
            _replaceWayTagFlushCommand.Parameters.Add(@"value", MySqlDbType.String);

            _cached_way_tags = new List<Tuple<long?, Tag>>();
        }

        /// <summary>
        /// Initializes the replace way nodes command and cache
        /// </summary>
        /// <param name="connection">The MySQL connection to use</param>
        private void InitializeReplaceWayNodesCacheAndCommand(MySqlConnection connection)
        {
            var sql_string = REPLACE_WAY_NODES_BATCH_PREFIX;

            for (int i = 0; i < BATCH_WAY_NODES_COUNT; ++i)
            {
                sql_string += String.Format(REPLACE_WAY_NODES_BATCH_VALUES, i);

                if (i != BATCH_WAY_NODES_COUNT - 1)
                {
                    sql_string += @",";
                }
            }

            sql_string += @";";

            _replaceWayNodesBatchCommand = new MySqlCommand(sql_string, connection);

            for (int i = 0; i < BATCH_WAY_NODES_COUNT; ++i)
            {
                _replaceWayNodesBatchCommand.Parameters.Add(@"way_id" + i, MySqlDbType.Int64);
                _replaceWayNodesBatchCommand.Parameters.Add(@"node_id" + i, MySqlDbType.Int64);
                _replaceWayNodesBatchCommand.Parameters.Add(@"sequence_id" + i, MySqlDbType.Int64);
            }

            sql_string = REPLACE_WAY_NODES;

            _replaceWayNodeFlushCommand = new MySqlCommand(sql_string, connection);

            _replaceWayNodeFlushCommand.Parameters.Add(@"way_id", MySqlDbType.Int64);
            _replaceWayNodeFlushCommand.Parameters.Add(@"node_id", MySqlDbType.Int64);
            _replaceWayNodeFlushCommand.Parameters.Add(@"sequence_id", MySqlDbType.Int64);

            _cached_way_nodes = new List<Tuple<long?, long?, long?>>();
        }

        /// <summary>
        /// Initializes the replace relation command and cache
        /// </summary>
        /// <param name="connection">The MySQL connection to use</param>
        private void InitializeReplaceRelationCacheAndCommand(MySqlConnection connection)
        {
            var sql_string = REPLACE_RELATION_BATCH_PREFIX;

            for (int i = 0; i < BATCH_RELATIONS_COUNT; ++i)
            {
                sql_string += String.Format(REPLACE_RELATION_BATCH_VALUES, i);

                if (i != BATCH_RELATIONS_COUNT - 1)
                {
                    sql_string += @",";
                }
            }

            sql_string += @";";

            _replaceRelationsBatchCommand = new MySqlCommand(sql_string, connection);

            for (int i = 0; i < BATCH_RELATIONS_COUNT; ++i)
            {
                _replaceRelationsBatchCommand.Parameters.Add(@"@id" + i, MySqlDbType.Int64);
                _replaceRelationsBatchCommand.Parameters.Add(@"@changeset_id" + i, MySqlDbType.Int64);
                _replaceRelationsBatchCommand.Parameters.Add(@"@visible" + i, MySqlDbType.Bit);
                _replaceRelationsBatchCommand.Parameters.Add(@"@time_stamp" + i, MySqlDbType.DateTime);
                _replaceRelationsBatchCommand.Parameters.Add(@"@version" + i, MySqlDbType.Int64);
                _replaceRelationsBatchCommand.Parameters.Add(@"@usr" + i, MySqlDbType.String);
                _replaceRelationsBatchCommand.Parameters.Add(@"@usr_id" + i, MySqlDbType.Int32);
            }

            sql_string = REPLACE_RELATION;

            _replaceRelationFlushCommand = new MySqlCommand(sql_string, connection);

            _replaceRelationFlushCommand.Parameters.Add(@"@id", MySqlDbType.Int64);
            _replaceRelationFlushCommand.Parameters.Add(@"@changeset_id", MySqlDbType.Int64);
            _replaceRelationFlushCommand.Parameters.Add(@"@visible", MySqlDbType.Bit);
            _replaceRelationFlushCommand.Parameters.Add(@"@time_stamp", MySqlDbType.DateTime);
            _replaceRelationFlushCommand.Parameters.Add(@"@version", MySqlDbType.Int64);
            _replaceRelationFlushCommand.Parameters.Add(@"@usr", MySqlDbType.String);
            _replaceRelationFlushCommand.Parameters.Add(@"@usr_id", MySqlDbType.Int32);

            _cached_relations = new List<Relation>();
        }

        /// <summary>
        /// Initializes the replace relation tags command and cache
        /// </summary>
        /// <param name="connection">The MySQL connection to use</param>
        private void InitializeReplaceRelationTagsCacheAndCommand(MySqlConnection connection)
        {
            var sql_string = REPLACE_RELATION_TAGS_BATCH_PREFIX;

            for (int i = 0; i < BATCH_RELATION_TAGS_COUNT; ++i)
            {
                sql_string += String.Format(REPLACE_RELATION_TAGS_BATCH_VALUES, i);

                if (i != BATCH_RELATION_TAGS_COUNT - 1)
                {
                    sql_string += @",";
                }
            }

            sql_string += @";";

            _replaceRelationTagsBatchCommand = new MySqlCommand(sql_string, connection);

            for (int i = 0; i < BATCH_RELATION_TAGS_COUNT; ++i)
            {
                _replaceRelationTagsBatchCommand.Parameters.Add(@"relation_id" + i, MySqlDbType.Int64);
                _replaceRelationTagsBatchCommand.Parameters.Add(@"tag_key" + i, MySqlDbType.String);
                _replaceRelationTagsBatchCommand.Parameters.Add(@"value" + i, MySqlDbType.String);
            }

            sql_string = REPLACE_RELATION_TAGS;

            _replaceRelationTagFlushCommand = new MySqlCommand(sql_string, connection);

            _replaceRelationTagFlushCommand.Parameters.Add(@"relation_id", MySqlDbType.Int64);
            _replaceRelationTagFlushCommand.Parameters.Add(@"tag_key", MySqlDbType.String);
            _replaceRelationTagFlushCommand.Parameters.Add(@"value", MySqlDbType.String);

            _cached_relation_tags = new List<Tuple<long?, Tag>>();
        }

        /// <summary>
        /// Initializes the replace relation members command and cache
        /// </summary>
        /// <param name="connection">The MySQL connection to use</param>
        private void InitializeReplaceRelationMembersCacheAndCommand(MySqlConnection connection)
        {
            var sql_string = REPLACE_RELATION_MEMBERS_BATCH_PREFIX;

            for (int i = 0; i < BATCH_RELATION_MEMBERS_COUNT; ++i)
            {
                sql_string += String.Format(REPLACE_RELATION_MEMBERS_BATCH_VALUES, i);

                if (i != BATCH_RELATION_MEMBERS_COUNT - 1)
                {
                    sql_string += @",";
                }
            }

            sql_string += @";";

            _replaceRelationMembersBatchCommand = new MySqlCommand(sql_string, connection);

            for (int i = 0; i < BATCH_RELATION_MEMBERS_COUNT; ++i)
            {
                _replaceRelationMembersBatchCommand.Parameters.Add(@"relation_id" + i, MySqlDbType.Int64);
                _replaceRelationMembersBatchCommand.Parameters.Add(@"member_type" + i, MySqlDbType.UByte);
                _replaceRelationMembersBatchCommand.Parameters.Add(@"member_id" + i, MySqlDbType.Int64);
                _replaceRelationMembersBatchCommand.Parameters.Add(@"member_role" + i, MySqlDbType.String);
                _replaceRelationMembersBatchCommand.Parameters.Add(@"sequence_id" + i, MySqlDbType.Int64);
            }

            sql_string = REPLACE_RELATION_MEMBERS;

            _replaceRelationMemberFlushCommand = new MySqlCommand(sql_string, connection);

            _replaceRelationMemberFlushCommand.Parameters.Add(@"relation_id", MySqlDbType.Int64);
            _replaceRelationMemberFlushCommand.Parameters.Add(@"member_type", MySqlDbType.UByte);
            _replaceRelationMemberFlushCommand.Parameters.Add(@"member_id", MySqlDbType.Int64);
            _replaceRelationMemberFlushCommand.Parameters.Add(@"member_role", MySqlDbType.String);
            _replaceRelationMemberFlushCommand.Parameters.Add(@"sequence_id", MySqlDbType.Int64);

            _cached_relation_members = new List<Tuple<long?, RelationMember, long?>>();
        }

        #endregion initialization

        #region add methods

        /// <summary>
        /// Adds a node.
        /// </summary>
        /// <param name="node">The node to add</param>
        public override void AddNode(Node node)
        {
            if (_cached_nodes.Count == BATCH_NODES_COUNT)
            {
                BatchAddNodes(_cached_nodes);
            }

            _cached_nodes.Add(node);

            AddNodeTags(node);
        }

        /// <summary>
        /// Adds a nodes tags
        /// </summary>
        /// <param name="node">The node to add tags to the db from</param>
        private void AddNodeTags(Node node)
        {
            if (node.Tags != null)
            {
                foreach (var tag in node.Tags)
                {
                    if (_cached_node_tags.Count == BATCH_NODE_TAGS_COUNT)
                    {
                        BatchAddNodeTags(_cached_node_tags);
                    }

                    _cached_node_tags.Add(new Tuple<long?, Tag>(node.Id, tag));
                }
            }
        }

        /// <summary>
        /// Adds a way.
        /// </summary>
        /// <param name="way">The way to add to the db</param>
        public override void AddWay(Way way)
        {
            if (_cached_ways.Count == BATCH_WAYS_COUNT)
            {
                BatchAddWays(_cached_ways);
            }

            _cached_ways.Add(way);

            AddWayTags(way);
            AddWayNodes(way);
        }

        /// <summary>
        /// Adds a ways tags
        /// </summary>
        /// <param name="way">The way to add tags to the db from</param>
        private void AddWayTags(Way way)
        {
            if (way.Tags != null)
            {
                foreach (var tag in way.Tags)
                {
                    if (_cached_way_tags.Count == BATCH_WAY_TAGS_COUNT)
                    {
                        BatchAddWayTags(_cached_way_tags);
                    }

                    _cached_way_tags.Add(new Tuple<long?, Tag>(way.Id, tag));
                }
            }
        }

        /// <summary>
        /// Adds a ways member nodes
        /// </summary>
        /// <param name="way">The way to add member nodes to the db from</param>
        private void AddWayNodes(Way way)
        {
            if (way.Nodes != null)
            {
                long? sequence_id = 0;
                foreach (var node in way.Nodes)
                {
                    if(_cached_way_nodes.Count == BATCH_WAY_NODES_COUNT)
                    {
                        BatchAddWayNodes(_cached_way_nodes);
                    }

                    _cached_way_nodes.Add(new Tuple<long?, long?, long?>(way.Id, node, sequence_id));
                    ++sequence_id;
                }
            }
        }

        /// <summary>
        /// Adds a relation.
        /// </summary>
        /// <param name="relation">The relation to add to the db</param>
        public override void AddRelation(Relation relation)
        {
            if (_cached_relations.Count == BATCH_RELATIONS_COUNT)
            {
                BatchAddRelations(_cached_relations);
            }

            _cached_relations.Add(relation);

            AddRelationTags(relation);
            AddRelationMembers(relation);
        }

        /// <summary>
        /// Adds a relations tags
        /// </summary>
        /// <param name="relation">The relation to add tags to the db from</param>
        private void AddRelationTags(Relation relation)
        {
            if (relation.Tags != null)
            {
                foreach (var tag in relation.Tags)
                {
                    if (_cached_relation_tags.Count == BATCH_RELATION_TAGS_COUNT)
                    {
                        BatchAddRelationTags(_cached_relation_tags);
                    }

                    _cached_relation_tags.Add(new Tuple<long?, Tag>(relation.Id, tag));                    
                }
            }
        }

        /// <summary>
        /// Adds a relations members
        /// </summary>
        /// <param name="relation">The relation to add members to the db from</param>
        private void AddRelationMembers(Relation relation)
        {
            if (relation.Members != null)
            {
                long? sequence_id = 0;
                foreach(var relation_member in relation.Members)
                {
                    if (_cached_relation_members.Count == BATCH_RELATION_MEMBERS_COUNT)
                    {
                        BatchAddRelationMembers(_cached_relation_members);
                    }

                    _cached_relation_members.Add(new Tuple<long?, RelationMember, long?>(relation.Id, relation_member, sequence_id));
                    ++sequence_id;
                }
            }
        }

        #endregion add methods

        #region batch methods

        /// <summary>
        /// Batch adds the queued nodes to the MySQL db
        /// </summary>
        private void BatchAddNodes(List<Node> nodes)
        {
            int i = 0;
            foreach (var node in nodes)
            {
                _replaceNodeBatchCommand.Parameters[(i * 10) + 0].Value = node.Id.ConvertToDBValue<long>();

                int? latitude = MySQLSchemaTools.GeoToDB((double)node.Latitude);
                _replaceNodeBatchCommand.Parameters[(i * 10) + 1].Value = latitude.ConvertToDBValue<int>();

                int? longitude = MySQLSchemaTools.GeoToDB((double)node.Longitude);
                _replaceNodeBatchCommand.Parameters[(i * 10) + 2].Value = longitude.ConvertToDBValue<int>();

                _replaceNodeBatchCommand.Parameters[(i * 10) + 3].Value = node.ChangeSetId.ConvertToDBValue<long>();

                if (node.Visible.HasValue)
                {
                    _replaceNodeBatchCommand.Parameters[(i * 10) + 4].Value = node.Visible.Value ? 1 : 0;
                }
                else
                {
                    _replaceNodeBatchCommand.Parameters[(i * 10) + 4].Value = DBNull.Value;
                }

                _replaceNodeBatchCommand.Parameters[(i * 10) + 5].Value = node.TimeStamp.ConvertToDBValue<DateTime>();
                _replaceNodeBatchCommand.Parameters[(i * 10) + 6].Value = Tile.CreateAroundLocation(new Math.Geo.GeoCoordinate(node.Latitude.Value, node.Longitude.Value), MySQLSchemaTools.DefaultTileZoomLevel).Id;
                _replaceNodeBatchCommand.Parameters[(i * 10) + 7].Value = node.Version.ConvertToDBValue<ulong>();
                var user_name = node.UserName;
                _replaceNodeBatchCommand.Parameters[(i * 10) + 8].Value = user_name.Truncate(100);
                _replaceNodeBatchCommand.Parameters[(i * 10) + 9].Value = node.UserId.ConvertToDBValue<long>();

                ++i;
            }

            _replaceNodeBatchCommand.ExecuteNonQuery();

            _total_node_count += nodes.Count;
            nodes.Clear();
        }

        /// <summary>
        /// Flushes out the remaining nodes
        /// </summary>
        private void FlushAddNodes(List<Node> nodes)
        {
            foreach (var node in nodes)
            {
                _replaceNodeFlushCommand.Parameters[0].Value = node.Id.ConvertToDBValue<long>();

                int? latitude = MySQLSchemaTools.GeoToDB((double)node.Latitude);
                _replaceNodeFlushCommand.Parameters[1].Value = latitude.ConvertToDBValue<int>();

                int? longitude = MySQLSchemaTools.GeoToDB((double)node.Longitude);
                _replaceNodeFlushCommand.Parameters[2].Value = longitude.ConvertToDBValue<int>();

                _replaceNodeFlushCommand.Parameters[3].Value = node.ChangeSetId.ConvertToDBValue<long>();

                if (node.Visible.HasValue)
                {
                    _replaceNodeFlushCommand.Parameters[4].Value = node.Visible.Value ? 1 : 0;
                }
                else
                {
                    _replaceNodeFlushCommand.Parameters[4].Value = DBNull.Value;
                }

                _replaceNodeFlushCommand.Parameters[5].Value = node.TimeStamp.ConvertToDBValue<DateTime>();
                _replaceNodeFlushCommand.Parameters[6].Value = Tile.CreateAroundLocation(new Math.Geo.GeoCoordinate(node.Latitude.Value, node.Longitude.Value), MySQLSchemaTools.DefaultTileZoomLevel).Id;
                _replaceNodeFlushCommand.Parameters[7].Value = node.Version.ConvertToDBValue<ulong>();
                var user_name = node.UserName;
                _replaceNodeFlushCommand.Parameters[8].Value = user_name.Truncate(100);
                _replaceNodeFlushCommand.Parameters[9].Value = node.UserId.ConvertToDBValue<long>();

                _replaceNodeFlushCommand.ExecuteNonQuery();                
            }

            _total_node_count += nodes.Count;
            nodes.Clear();
        }

        /// <summary>
        /// Batch adds the queued node tags to the MySQL db
        /// </summary>
        private void BatchAddNodeTags(List<Tuple<long?, Tag>> node_tags)
        {
            int i = 0;
            foreach (var tag in node_tags)
            {
                var key = tag.Item2.Key;
                var value = tag.Item2.Value;

                _replaceNodeTagsBatchCommand.Parameters[(i * 3) + 0].Value = tag.Item1.ConvertToDBValue<long>();
                _replaceNodeTagsBatchCommand.Parameters[(i * 3) + 1].Value = key.Truncate(100);
                _replaceNodeTagsBatchCommand.Parameters[(i * 3) + 2].Value = value.Truncate(500);

                ++i;
            }

            _replaceNodeTagsBatchCommand.ExecuteNonQuery();

            _total_node_tags_count += node_tags.Count;
            node_tags.Clear();
        }

        /// <summary>
        /// Flushes out remaining node tags
        /// </summary>
        private void FlushAddNodeTags(List<Tuple<long?, Tag>> node_tags)
        {
            foreach (var tag in node_tags)
            {
                var key = tag.Item2.Key;
                var value = tag.Item2.Value;

                _replaceNodeTagFlushCommand.Parameters[0].Value = tag.Item1.ConvertToDBValue<long>();
                _replaceNodeTagFlushCommand.Parameters[1].Value = key.Truncate(100);
                _replaceNodeTagFlushCommand.Parameters[2].Value = value.Truncate(500);

                _replaceNodeTagFlushCommand.ExecuteNonQuery();
            }

            _total_node_tags_count += node_tags.Count;
            node_tags.Clear();
        }

        /// <summary>
        /// Batch adds the ways to the MySQL db
        /// </summary>
        private void BatchAddWays(List<Way> ways)
        {
            int i = 0;
            foreach (var way in ways)
            {
                _replaceWayBatchCommand.Parameters[(i * 7) + 0].Value = way.Id.ConvertToDBValue<long>();
                _replaceWayBatchCommand.Parameters[(i * 7) + 1].Value = way.ChangeSetId.ConvertToDBValue<long>();

                if (way.Visible.HasValue)
                {
                    _replaceWayBatchCommand.Parameters[(i * 7) + 2].Value = way.Visible.Value ? 1 : 0;
                }
                else
                {
                    _replaceWayBatchCommand.Parameters[(i * 7) + 2].Value = DBNull.Value;
                }

                _replaceWayBatchCommand.Parameters[(i * 7) + 3].Value = way.TimeStamp.ConvertToDBValue<DateTime>();
                _replaceWayBatchCommand.Parameters[(i * 7) + 4].Value = way.Version.ConvertToDBValue<ulong>();
                var user_name = way.UserName;
                _replaceWayBatchCommand.Parameters[(i * 7) + 5].Value = user_name.Truncate(100);
                _replaceWayBatchCommand.Parameters[(i * 7) + 6].Value = way.UserId.ConvertToDBValue<long>();

                ++i;
            }

            _replaceWayBatchCommand.ExecuteNonQuery();

            _total_way_count += ways.Count;
            ways.Clear();
        }

        /// <summary>
        /// Flushes out remaining ways
        /// </summary>
        private void FlushAddWays(List<Way> ways)
        {
            foreach (var way in ways)
            {
                _replaceWayFlushCommand.Parameters[0].Value = way.Id.ConvertToDBValue<long>();
                _replaceWayFlushCommand.Parameters[1].Value = way.ChangeSetId.ConvertToDBValue<long>();

                if (way.Visible.HasValue)
                {
                    _replaceWayFlushCommand.Parameters[2].Value = way.Visible.Value ? 1 : 0;
                }
                else
                {
                    _replaceWayFlushCommand.Parameters[2].Value = DBNull.Value;
                }

                _replaceWayFlushCommand.Parameters[3].Value = way.TimeStamp.ConvertToDBValue<DateTime>();
                _replaceWayFlushCommand.Parameters[4].Value = way.Version.ConvertToDBValue<ulong>();
                var user_name = way.UserName;
                _replaceWayFlushCommand.Parameters[5].Value = user_name.Truncate(100);
                _replaceWayFlushCommand.Parameters[6].Value = way.UserId.ConvertToDBValue<long>();

                _replaceWayFlushCommand.ExecuteNonQuery();
            }
            
            _total_way_count += ways.Count;
            ways.Clear();
        }

        /// <summary>
        /// Batch adds the way tags to the MySQL db
        /// </summary>
        private void BatchAddWayTags(List<Tuple<long?, Tag>> way_tags)
        {
            int i = 0;
            foreach (var tag in way_tags)
            {
                var key = tag.Item2.Key;
                var value = tag.Item2.Value;

                _replaceWayTagsBatchCommand.Parameters[(i * 3) + 0].Value = tag.Item1.ConvertToDBValue<long>();
                _replaceWayTagsBatchCommand.Parameters[(i * 3) + 1].Value = key.Truncate(100);
                _replaceWayTagsBatchCommand.Parameters[(i * 3) + 2].Value = value.Truncate(500);

                ++i;
            }

            _replaceWayTagsBatchCommand.ExecuteNonQuery();

            _total_way_tags_count += way_tags.Count;
            way_tags.Clear();
        }

        /// <summary>
        /// Flushes out remaining way tags
        /// </summary>
        private void FlushAddWayTags(List<Tuple<long?, Tag>> way_tags)
        {
            foreach (var tag in way_tags)
            {
                var key = tag.Item2.Key;
                var value = tag.Item2.Value;

                _replaceWayTagFlushCommand.Parameters[0].Value = tag.Item1.ConvertToDBValue<long>();
                _replaceWayTagFlushCommand.Parameters[1].Value = key.Truncate(100);
                _replaceWayTagFlushCommand.Parameters[2].Value = value.Truncate(500);

                _replaceWayTagFlushCommand.ExecuteNonQuery();
            }            

            _total_way_tags_count += way_tags.Count;
            way_tags.Clear();
        }

        /// <summary>
        /// Batch adds the way nodes to the MySQL db
        /// </summary>
        private void BatchAddWayNodes(List<Tuple<long?, long?, long?>> way_nodes)
        {
            int i = 0;
            foreach (var way_node in way_nodes)
            {
                _replaceWayNodesBatchCommand.Parameters[(i * 3) + 0].Value = way_node.Item1.ConvertToDBValue<long>();
                _replaceWayNodesBatchCommand.Parameters[(i * 3) + 1].Value = way_node.Item2.ConvertToDBValue<long>();
                _replaceWayNodesBatchCommand.Parameters[(i * 3) + 2].Value = way_node.Item3.ConvertToDBValue<long>();

                ++i;
            }

            _replaceWayNodesBatchCommand.ExecuteNonQuery();

            _total_way_nodes_count += way_nodes.Count;
            way_nodes.Clear();
        }

        /// <summary>
        /// Flushes out remaining way tags
        /// </summary>
        private void FlushAddWayNodes(List<Tuple<long?, long?, long?>> way_nodes)
        {
            foreach (var way_node in way_nodes)
            {
                _replaceWayNodeFlushCommand.Parameters[0].Value = way_node.Item1.ConvertToDBValue<long>();
                _replaceWayNodeFlushCommand.Parameters[1].Value = way_node.Item2.ConvertToDBValue<long>();
                _replaceWayNodeFlushCommand.Parameters[2].Value = way_node.Item3.ConvertToDBValue<long>();

                _replaceWayNodeFlushCommand.ExecuteNonQuery();
            }            

            _total_way_nodes_count += way_nodes.Count;
            way_nodes.Clear();
        }

        /// <summary>
        /// Batch adds the relations to the MySQL db
        /// </summary>
        private void BatchAddRelations(List<Relation> relations)
        {
            int i = 0;
            foreach (var relation in relations)
            {
                _replaceRelationsBatchCommand.Parameters[(i * 7) + 0].Value = relation.Id.ConvertToDBValue<long>();
                _replaceRelationsBatchCommand.Parameters[(i * 7) + 1].Value = relation.ChangeSetId.ConvertToDBValue<long>();

                if (relation.Visible.HasValue)
                {
                    _replaceRelationsBatchCommand.Parameters[(i * 7) + 2].Value = relation.Visible.Value ? 1 : 0;
                }
                else
                {
                    _replaceRelationsBatchCommand.Parameters[(i * 7) + 2].Value = DBNull.Value;
                }

                _replaceRelationsBatchCommand.Parameters[(i * 7) + 3].Value = relation.TimeStamp.ConvertToDBValue<DateTime>();
                _replaceRelationsBatchCommand.Parameters[(i * 7) + 4].Value = relation.Version.ConvertToDBValue<ulong>();
                var user_name = relation.UserName;
                _replaceRelationsBatchCommand.Parameters[(i * 7) + 5].Value = user_name.Truncate(100);
                _replaceRelationsBatchCommand.Parameters[(i * 7) + 6].Value = relation.UserId.ConvertToDBValue<long>();

                ++i;
            }

            _replaceRelationsBatchCommand.ExecuteNonQuery();

            _total_relation_count += relations.Count;
            relations.Clear();
        }

        /// <summary>
        /// Flushes out remaining relations
        /// </summary>
        private void FlushAddRelations(List<Relation> relations)
        {
            foreach (var relation in relations)
            {
                _replaceRelationFlushCommand.Parameters[0].Value = relation.Id.ConvertToDBValue<long>();
                _replaceRelationFlushCommand.Parameters[1].Value = relation.ChangeSetId.ConvertToDBValue<long>();

                if (relation.Visible.HasValue)
                {
                    _replaceRelationFlushCommand.Parameters[2].Value = relation.Visible.Value ? 1 : 0;
                }
                else
                {
                    _replaceRelationFlushCommand.Parameters[2].Value = DBNull.Value;
                }

                _replaceRelationFlushCommand.Parameters[3].Value = relation.TimeStamp.ConvertToDBValue<DateTime>();
                _replaceRelationFlushCommand.Parameters[4].Value = relation.Version.ConvertToDBValue<ulong>();
                string user_name = relation.UserName;
                _replaceRelationFlushCommand.Parameters[5].Value = user_name.Truncate(100);
                _replaceRelationFlushCommand.Parameters[6].Value = relation.UserId.ConvertToDBValue<long>();

                _replaceRelationFlushCommand.ExecuteNonQuery();
            }            

            _total_relation_count += relations.Count;
            relations.Clear();
        }

        /// <summary>
        /// Batch adds the relation tags to the MySQL db
        /// </summary>
        private void BatchAddRelationTags(List<Tuple<long?, Tag>> relation_tags)
        {
            int i = 0;
            foreach (var tag in relation_tags)
            {
                string key = tag.Item2.Key;
                string value = tag.Item2.Value;

                _replaceRelationTagsBatchCommand.Parameters[(i * 3) + 0].Value = tag.Item1.ConvertToDBValue<long>();
                _replaceRelationTagsBatchCommand.Parameters[(i * 3) + 1].Value = key.Truncate(100);
                _replaceRelationTagsBatchCommand.Parameters[(i * 3) + 2].Value = value.Truncate(500);

                ++i;
            }

            _replaceRelationTagsBatchCommand.ExecuteNonQuery();

            _total_relation_tags_count += relation_tags.Count;
            relation_tags.Clear();
        }

        /// <summary>
        /// Flushes out remaining relation tags
        /// </summary>
        private void FlushAddRelationTags(List<Tuple<long?, Tag>> relation_tags)
        {
            foreach (var tag in relation_tags)
            {
                var key = tag.Item2.Key;
                var value = tag.Item2.Value;

                _replaceRelationTagFlushCommand.Parameters[0].Value = tag.Item1.ConvertToDBValue<long>();
                _replaceRelationTagFlushCommand.Parameters[1].Value = key.Truncate(100);
                _replaceRelationTagFlushCommand.Parameters[2].Value = value.Truncate(500);

                _replaceRelationTagFlushCommand.ExecuteNonQuery();
            }

            _total_relation_tags_count += relation_tags.Count;
            relation_tags.Clear();
        }

        /// <summary>
        /// Batch adds the relation members to the MySQL db
        /// </summary>
        private void BatchAddRelationMembers(List<Tuple<long?, RelationMember, long?>> relation_members)
        {
            int i = 0;
            foreach (var relation_member in relation_members)
            {
                _replaceRelationMembersBatchCommand.Parameters[(i * 5) + 0].Value = relation_member.Item1.ConvertToDBValue<long>();

                if (relation_member.Item2.MemberType == OsmGeoType.Node)
                {
                    _replaceRelationMembersBatchCommand.Parameters[(i * 5) + 1].Value = 0;
                }
                else if (relation_member.Item2.MemberType == OsmGeoType.Way)
                {
                    _replaceRelationMembersBatchCommand.Parameters[(i * 5) + 1].Value = 1;
                }
                else if (relation_member.Item2.MemberType == OsmGeoType.Relation)
                {
                    _replaceRelationMembersBatchCommand.Parameters[(i * 5) + 1].Value = 2;
                }

                _replaceRelationMembersBatchCommand.Parameters[(i * 5) + 2].Value = relation_member.Item2.MemberId.ConvertToDBValue<long>();
                var member_role = relation_member.Item2.MemberRole;
                _replaceRelationMembersBatchCommand.Parameters[(i * 5) + 3].Value = member_role.Truncate(100);
                _replaceRelationMembersBatchCommand.Parameters[(i * 5) + 4].Value = relation_member.Item3.ConvertToDBValue<long>();

                ++i;
            }

            _replaceRelationMembersBatchCommand.ExecuteNonQuery();

            _total_relation_members_count += relation_members.Count;
            relation_members.Clear();
        }

        /// <summary>
        /// Flushes out remaining relation members
        /// </summary>
        private void FlushAddRelationMembers(List<Tuple<long?, RelationMember, long?>> relation_members)
        {
            foreach (var relation_member in relation_members)
            {
                _replaceRelationMemberFlushCommand.Parameters[0].Value = relation_member.Item1.ConvertToDBValue<long>();

                if (relation_member.Item2.MemberType == OsmGeoType.Node)
                {
                    _replaceRelationMemberFlushCommand.Parameters[1].Value = 0;
                }
                else if (relation_member.Item2.MemberType == OsmGeoType.Way)
                {
                    _replaceRelationMemberFlushCommand.Parameters[1].Value = 1;
                }
                else if (relation_member.Item2.MemberType == OsmGeoType.Relation)
                {
                    _replaceRelationMemberFlushCommand.Parameters[1].Value = 2;
                }

                _replaceRelationMemberFlushCommand.Parameters[2].Value = relation_member.Item2.MemberId.ConvertToDBValue<long>();
                var member_role = relation_member.Item2.MemberRole;
                _replaceRelationMemberFlushCommand.Parameters[3].Value = member_role.Truncate(100);
                _replaceRelationMemberFlushCommand.Parameters[4].Value = relation_member.Item3.ConvertToDBValue<long>();

                _replaceRelationMemberFlushCommand.ExecuteNonQuery();
            }

            _total_relation_members_count += relation_members.Count;
            relation_members.Clear();            
        }

        #endregion batch methods

        /// <summary>
        /// Returns a copy of the MySQL target that will safely allow
        /// concurrent writing alongside the MySQL target it was copied
        /// from
        /// </summary>
        /// <returns>The concurrent safe MySQL target</returns>
        public override OsmStreamTarget ConcurrentCopy()
        {            
            return new MySQLOsmStreamTarget(_connection_string);
        }

        /// <summary>
        /// Closes this target.
        /// </summary>
        public override void Close()
        {
            _replaceNodeBatchCommand.Dispose();
            _replaceNodeFlushCommand.Dispose();
            _replaceNodeTagsBatchCommand.Dispose();
            _replaceNodeTagFlushCommand.Dispose();
            _replaceWayBatchCommand.Dispose();
            _replaceWayFlushCommand.Dispose();
            _replaceWayTagsBatchCommand.Dispose();
            _replaceWayTagFlushCommand.Dispose();
            _replaceWayNodesBatchCommand.Dispose();
            _replaceWayNodeFlushCommand.Dispose();
            _replaceRelationsBatchCommand.Dispose();
            _replaceRelationFlushCommand.Dispose();
            _replaceRelationTagsBatchCommand.Dispose();
            _replaceRelationTagFlushCommand.Dispose();
            _replaceRelationMembersBatchCommand.Dispose();
            _replaceRelationMemberFlushCommand.Dispose();
            if (_connection_owner)
            {
                if (_connection != null && !Utilities.IsNullOrWhiteSpace(_connection_string))
                {
                    _connection.Close();
                    _connection.Dispose();

                    _connection = null;
                }
            }
        }

        /// <summary>
        /// Flushes all data.
        /// </summary>
        public override void Flush()
        {
            if (_cached_nodes.Count > 0)
            {
                FlushAddNodes(_cached_nodes);

                OsmSharp.Logging.Log.TraceEvent("OsmSharp.Data.MySQL.Osm.Streams.MySQLOsmStreamTarget", OsmSharp.Logging.TraceEventType.Information,
                "Inserted {0} records into {1}!", _total_node_count, "node");
            }

            if (_cached_node_tags.Count > 0)
            {
                FlushAddNodeTags(_cached_node_tags);

                OsmSharp.Logging.Log.TraceEvent("OsmSharp.Data.MySQL.Osm.Streams.MySQLOsmStreamTarget", OsmSharp.Logging.TraceEventType.Information,
                "Inserted {0} records into {1}!", _total_node_tags_count, "node_tags");
            }

            if (_cached_ways.Count > 0)
            {
                FlushAddWays(_cached_ways);

                OsmSharp.Logging.Log.TraceEvent("OsmSharp.Data.MySQL.Osm.Streams.MySQLOsmStreamTarget", OsmSharp.Logging.TraceEventType.Information,
                "Inserted {0} records into {1}!", _total_way_count, "way");
            }
            
            if (_cached_way_tags.Count > 0)
            {
                FlushAddWayTags(_cached_way_tags);

                OsmSharp.Logging.Log.TraceEvent("OsmSharp.Data.MySQL.Osm.Streams.MySQLOsmStreamTarget", OsmSharp.Logging.TraceEventType.Information,
                "Inserted {0} records into {1}!", _total_way_tags_count, "way_tags");
            }
            
            if (_cached_way_nodes.Count > 0)
            {
                FlushAddWayNodes(_cached_way_nodes);

                OsmSharp.Logging.Log.TraceEvent("OsmSharp.Data.MySQL.Osm.Streams.MySQLOsmStreamTarget", OsmSharp.Logging.TraceEventType.Information,
                "Inserted {0} records into {1}!", _total_way_nodes_count, "way_nodes");
            }
            
            if (_cached_relations.Count > 0)
            {
                FlushAddRelations(_cached_relations);

                OsmSharp.Logging.Log.TraceEvent("OsmSharp.Data.MySQL.Osm.Streams.MySQLOsmStreamTarget", OsmSharp.Logging.TraceEventType.Information,
                "Inserted {0} records into {1}!", _total_relation_count, "relation");
            }
            
            if (_cached_relation_tags.Count > 0)
            {
                FlushAddRelationTags(_cached_relation_tags);

                OsmSharp.Logging.Log.TraceEvent("OsmSharp.Data.MySQL.Osm.Streams.MySQLOsmStreamTarget", OsmSharp.Logging.TraceEventType.Information,
                "Inserted {0} records into {1}!", _total_relation_tags_count, "relation_tags");
            }
            
            if (_cached_relation_members.Count > 0)
            {
                FlushAddRelationMembers(_cached_relation_members);

                OsmSharp.Logging.Log.TraceEvent("OsmSharp.Data.MySQL.Osm.Streams.MySQLOsmStreamTarget", OsmSharp.Logging.TraceEventType.Information,
                "Inserted {0} records into {1}!", _total_relation_members_count, "relation_members");
            }
        }
    }
}
