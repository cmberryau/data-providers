// OsmSharp - OpenSteetMap (OSM) SDK
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
using OsmSharp.Collections.Tags;
using OsmSharp.Math.Geo;
using OsmSharp.Osm;
using OsmSharp.Osm.Collections;
using OsmSharp.Osm.Data;
using OsmSharp.Osm.Filters;
using OsmSharp.Osm.Tiles;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace OsmSharp.Data.MySQL.Osm
{
    /// <summary>
    /// A MySQL data source
    /// </summary>
    public class MySQLDataSource : DataSourceReadOnlyBase, IDisposable
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
        /// Returns true if this datasource supports concurrent copies.
        /// </summary>
        public override bool SupportsConcurrentCopies
        {
            get
            {
                return true;
            }
        }

        /// <summary>
        /// The default zoom level that this data source reads at
        /// </summary>
        public override int DefaultZoomLevel
        {
            get
            {

                return MySQLSchemaTools.DefaultTileZoomLevel;
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
        /// The id of this datasource.
        /// </summary>
        private Guid _id;

        /// <summary>
        /// Flag that indicates if the schema needs to be created if not present.
        /// </summary>
        private bool _create_and_detect_schema;

        /// <summary>
        /// Holds the connection to the MySQL db.
        /// </summary>
        private MySqlConnection _connection;

        #region command strings

        /// <summary>
        /// MySQL command string stub to select nodes
        /// </summary>
        private const string SELECT_NODES_STUB = @"SELECT " +
                                                     @"node.id, " +
                                                     @"node.latitude, " +
                                                     @"node.longitude, " +
                                                     @"node.changeset_id, " +
                                                     @"node.visible, " +
                                                     @"node.time_stamp, " +
                                                     @"node.tile, " +
                                                     @"node.version, " +
                                                     @"node.usr, " +
                                                     @"node.usr_id " +
                                                 @"FROM " +
                                                     @"node ";

        /// <summary>
        /// MySQL command string to select nodes from ids
        /// </summary>
        private const string SELECT_NODES = SELECT_NODES_STUB + @"WHERE node.id IN ({0});";

        /// <summary>
        /// MySQL command string to select nodes based on tiles
        /// </summary>
        private const string SELECT_NODES_FROM_TILES = SELECT_NODES_STUB + @"WHERE node.tile IN ({0});";

        /// <summary>
        /// MySQL command string to select nodes based on tiles and geocoords
        /// </summary>
        private const string SELECT_NODES_FROM_TILES_AND_GEO_COORDS = SELECT_NODES_STUB +
                                                                      @"WHERE node.tile IN ({0})" +
                                                                      @"AND node.visible = true " +
                                                                      @"AND (node.latitude >= {1} " +
                                                                      @"AND node.latitude < {2} " +
                                                                      @"AND node.longitude >= {3} " +
                                                                      @"AND node.longitude < {4});";

        /// <summary>
        /// MySQL command string to select node tags
        /// </summary>
        private const string SELECT_NODE_TAGS = @"SELECT " +
                                                    @"node_tags.tag_key, " +
                                                    @"node_tags.value " +
                                                @"FROM " +
                                                    @"node_tags " +
                                                @"WHERE node_tags.node_id IN ({0});";

        /// <summary>
        /// MySQL command string to select node tags and node id from a list of nodes
        /// </summary>
        private const string SELECT_NODES_IDS_TAGS = @"SELECT " +
                                                         @"node_tags.node_id, " +
                                                         @"node_tags.tag_key, " +
                                                         @"node_tags.value " +
                                                     @"FROM " +
                                                         @"node_tags " +
                                                     @"WHERE node_tags.node_id IN ({0});";

        /// <summary>
        /// MySQL command string to select ways
        /// </summary>
        private const string SELECT_WAYS = @"SELECT " +
                                                @"way.id, " +
                                                @"way.changeset_id, " +
                                                @"way.visible, " +
                                                @"way.time_stamp, " +
                                                @"way.version, " +
                                                @"way.usr, " +
                                                @"way.usr_id " +
                                            @"FROM " +
                                                @"way " +
                                            @"WHERE way.id IN ({0});";

        /// <summary>
        /// MySQL command string to select way tags from a list of way ids
        /// </summary>
        private const string SELECT_WAY_TAGS = @"SELECT " +
                                                   @"way_tags.tag_key, " +
                                                   @"way_tags.value " +
                                               @"FROM " +
                                                   @"way_tags " +
                                               @"WHERE way_tags.way_id IN ({0});";

        /// <summary>
        /// MySQL command string to select way tags and way id from a list of way ids
        /// </summary>
        private const string SELECT_WAYS_IDS_TAGS = @"SELECT " +
                                                        @"way_tags.way_id, " +
                                                        @"way_tags.tag_key, " +
                                                        @"way_tags.value " +
                                                    @"FROM " +
                                                        @"way_tags " +
                                                    @"WHERE way_tags.way_id IN ({0});";

        /// <summary>
        /// MySQL command string to select way nodes from a list of way ids
        /// </summary>
        private const string SELECT_WAY_NODES = @"SELECT " +
                                                    @"way_nodes.node_id " +
                                                @"FROM " +
                                                    @"way_nodes " +
                                                @"WHERE way_nodes.way_id IN ({0}) " +
                                                @"ORDER BY way_nodes.sequence_id;";

        /// <summary>
        /// MySQL command string to select way nodes and way ids from a list of way ids
        /// </summary>
        private const string SELECT_WAYS_IDS_NODES = @"SELECT " +
                                                         @"way_nodes.way_id, " +
                                                         @"way_nodes.node_id " +
                                                     @"FROM " +
                                                         @"way_nodes " +
                                                     @"WHERE way_nodes.way_id IN ({0}) " +
                                                     @"ORDER BY way_nodes.sequence_id;";

        /// <summary>
        /// MySQL command string to select ways from nodes
        /// </summary>
        private const string SELECT_WAYS_FROM_NODES = @"SELECT " +
                                                          @"way_nodes.way_id " +
                                                      @"FROM " +
                                                          @"way_nodes " +
                                                      @"WHERE way_nodes.node_id IN ({0});";

        /// <summary>
        /// MySQL command string to select relations
        /// </summary>
        private const string SELECT_RELATIONS = @"SELECT " +
                                                    @"relation.id, " +
                                                    @"relation.changeset_id, " +
                                                    @"relation.visible, " +
                                                    @"relation.time_stamp, " +
                                                    @"relation.version, " +
                                                    @"relation.usr, " +
                                                    @"relation.usr_id " +
                                                @"FROM " +
                                                    @"relation " +
                                                @"WHERE relation.id IN ({0});";

        /// <summary>
        /// MySQL command string to select relation tags
        /// </summary>
        private const string SELECT_RELATION_TAGS = @"SELECT " +
                                                        @"relation_tags.tag_key, " +
                                                        @"relation_tags.value " +
                                                    @"FROM " +
                                                        @"relation_tags " +
                                                    @"WHERE relation_tags.relation_id IN ({0});";

        /// <summary>
        /// MySQL command string to select relation ids and tags
        /// </summary>
        private const string SELECT_RELATIONS_IDS_TAGS = @"SELECT " +
                                                             @"relation_tags.relation_id, " +
                                                             @"relation_tags.tag_key, " +
                                                             @"relation_tags.value " +
                                                         @"FROM " +
                                                             @"relation_tags " +
                                                         @"WHERE relation_tags.relation_id IN ({0});";

        /// <summary>
        /// MySQL command string to select way nodes
        /// </summary>
        private const string SELECT_RELATION_MEMBERS = @"SELECT " +
                                                           @"relation_members.member_type, " +
                                                           @"relation_members.member_id, " +
                                                           @"relation_members.member_role " +
                                                       @"FROM " +
                                                           @"relation_members " +
                                                       @"WHERE relation_members.relation_id IN ({0}) " +
                                                       @"ORDER BY relation_members.sequence_id;";

        /// <summary>
        /// MySQL command string to select way nodes
        /// </summary>
        private const string SELECT_RELATIONS_IDS_MEMBERS = @"SELECT " +
                                                                @"relation_members.relation_id, " +
                                                                @"relation_members.member_type, " +
                                                                @"relation_members.member_id, " +
                                                                @"relation_members.member_role " +
                                                            @"FROM " +
                                                                @"relation_members " +
                                                            @"WHERE relation_members.relation_id IN ({0}) " +
                                                            @"ORDER BY relation_members.sequence_id;";

        /// <summary>
        /// MySQL command string to select relations for a member of a certain type
        /// </summary>
        private const string SELECT_RELATIONS_FOR_MEMBER = @"SELECT " +
                                                               @"relation_members.relation_id " +
                                                           @"FROM " +
                                                               @"relation_members " +
                                                           @"WHERE relation_members.member_id IN ({0}) " +
                                                           @"AND relation_members.member_type = {1};";
        #endregion

        /// <summary>
        /// Creates a new MySQL data source
        /// </summary>
        /// <param name="connection_string">The connection string for the MySQL db</param>
        /// <param name="create_schema">Does the db schema need to be created?</param>
        public MySQLDataSource(string connection_string, bool create_schema = false)
        {
            _connection_string = connection_string;
            _id = Guid.NewGuid();
            _create_and_detect_schema = create_schema;
            _connection_owner = false;

            _connection = EnsureConnection();
        }

        /// <summary>
        /// Creates a new MySQL data source
        /// </summary>
        /// <param name="connection">The MySQL connection</param>
        /// <param name="password">The password used to connect to the MySQL data base</param>
        /// <param name="create_schema">Should we detect if the db tables exist and attempt to create them?</param>
        public MySQLDataSource(MySqlConnection connection, string password, bool create_schema = false)
        {
            _connection = connection;
            _id = Guid.NewGuid();

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

            _connection = EnsureConnection();
        }

        /// <summary>
        /// Creates a new MySQL data source
        /// </summary>
        /// <param name="hostname">The hostname of the MySQL server</param>
        /// <param name="database">The MySQL schema name</param>
        /// <param name="username">The username to connect with, sent over plain text</param>
        /// <param name="password">The password to connect with, sent over plain text</param>
        /// <param name="create_schema">Does the db schema need to be created?</param>
        public MySQLDataSource(string hostname,
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
            _id = Guid.NewGuid();
            _create_and_detect_schema = create_schema;
            _connection_owner = false;

            _connection = EnsureConnection();
        }

        /// <summary>
        /// Creates a new/gets the existing connection.
        /// </summary>
        /// <returns></returns>
        private MySqlConnection EnsureConnection()
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
            {   // attempts to create and detect tables
                MySQLSchemaTools.CreateAndDetect(_connection);
            }

            return _connection;
        }

        #region IDataSourceReadOnly Members

        /// <summary>
        /// Not supported.
        /// </summary>
        public override GeoCoordinateBox BoundingBox
        {
            get
            {
                throw new NotSupportedException();
            }
        }

        /// <summary>
        /// Returns the name.
        /// </summary>
        public string Name
        {
            get
            {
                return "MySQL Schema Source";
            }
        }

        /// <summary>
        /// Returns the id.
        /// </summary>
        public override Guid Id
        {
            get
            {
                return _id;
            }
        }

        /// <summary>
        /// Returns false; database sources have no bounding box.
        /// </summary>
        public override bool HasBoundingBox
        {
            get
            {
                return false;
            }
        }

        /// <summary>
        /// Return true; source is readonly.
        /// </summary>
        public override bool IsReadOnly
        {
            get
            {
                return true;
            }
        }

        /// <summary>
        /// Converts the member type id to the relation membertype enum.
        /// </summary>
        /// <param name="member_type"></param>
        /// <returns></returns>
        private OsmGeoType? ConvertMemberType(long member_type)
        {
            switch (member_type)
            {
                case (long)OsmGeoType.Node:
                    return OsmGeoType.Node;
                case (long)OsmGeoType.Way:
                    return OsmGeoType.Way;
                case (long)OsmGeoType.Relation:
                    return OsmGeoType.Relation;
            }
            throw new ArgumentOutOfRangeException("Invalid member type.");
        }

        /// <summary>
        /// Converts the member type id to the relation membertype enum.
        /// </summary>
        /// <param name="member_type"></param>
        /// <returns></returns>
        private OsmGeoType? ConvertMemberType(short member_type)
        {
            switch (member_type)
            {
                case (short)OsmGeoType.Node:
                    return OsmGeoType.Node;
                case (short)OsmGeoType.Way:
                    return OsmGeoType.Way;
                case (short)OsmGeoType.Relation:
                    return OsmGeoType.Relation;
            }
            throw new ArgumentOutOfRangeException("Invalid member type.");
        }

        /// <summary>
        /// Converts the member type to long.
        /// </summary>
        /// <param name="memberType"></param>
        /// <returns></returns>
        private long? ConvertMemberType(OsmGeoType? memberType)
        {
            if (memberType.HasValue)
            {
                return (long)memberType.Value;
            }
            return null;
        }

        /// <summary>
        /// Converts the member type to short.
        /// </summary>
        /// <param name="memberType"></param>
        /// <returns></returns>
        private short? ConvertMemberTypeShort(OsmGeoType? memberType)
        {
            if (memberType.HasValue)
            {
                return (short)memberType.Value;
            }
            return null;
        }

        /// <summary>
        /// Converts a given unix time to a DateTime object.
        /// </summary>
        private DateTime ConvertDateTime(long unixTime)
        {
            return unixTime.FromUnixTime();
        }

        /// <summary>
        /// Constructs an id list for SQL.
        /// </summary>
        /// <param name="ids"></param>
        /// <returns></returns>
        private string ConstructIdList(IList<long> ids)
        {
            return ConstructIdList(ids, 0, ids.Count);
        }

        /// <summary>
        /// Constructs an id list for SQL for only the specified section of ids.
        /// </summary>
        /// <param name="ids"></param>
        /// <param name="start_idx"></param>
        /// <param name="end_idx"></param>
        /// <returns></returns>
        private string ConstructIdList(IList<long> ids, int start_idx, int end_idx)
        {
            var sb = new StringBuilder();

            string return_string = string.Empty;
            if (ids.Count > 0 && ids.Count > start_idx)
            {
                sb.Append(ids[start_idx].ToString());

                for (int i = start_idx + 1; i < end_idx; i++)
                {
                    var id_string = ids[i].ToString();

                    sb.Append(",");
                    sb.Append(id_string);
                }
            }

            return sb.ToString();
        }

        /// <summary>
        /// Constructs an id list for SQL for only the specified section of ids.
        /// </summary>
        /// <param name="ids"></param>
        private string ConstructIdList(IEnumerable<long> ids)        
        {
            var ids_list = ids.ToList();

            return ConstructIdList(ids_list, 0, ids_list.Count);
        }

        /// <summary>
        /// Constructs a tile id list for SQL for the TileRange
        /// </summary>
        /// <param name="tile_range"></param>
        private static List<long> ConstructIdList(TileRange tile_range)
        {
            var tile_ids = new List<long>();

            foreach (Tile tile in tile_range)
            {
                tile_ids.Add((long)tile.Id);
            }
            return tile_ids;
        }

        /// <summary>
        /// Returns all the nodes matching the passed ids
        /// </summary>
        /// <param name="ids">The list of ids to search the db for</param>
        /// <param name="sql_command">The SQL command to execute</param>
        /// <param name="args">Additional arguments to put into the SQL command</param>
        /// <returns>Nodes which match the ids given</returns>
        private IList<Node> GetNodesGivenSQL(IList<long> ids, string sql_command, params object[] args)
        {
            var return_list = new List<Node>();
            if (ids.Count > 0)
            {
                var nodes = new Dictionary<long, Node>();
                for (int idx_1000 = 0; idx_1000 <= ids.Count / 1000; idx_1000++)
                {
                    int start_idx = idx_1000 * 1000;
                    int stop_idx = System.Math.Min((idx_1000 + 1) * 1000, ids.Count);
                    string ids_string = ConstructIdList(ids, start_idx, stop_idx);

                    if (ids_string.Length > 0)
                    {
                        string sql;

                        if (args.Length == 0)
                        {
                            sql = string.Format(sql_command, ids_string);
                        }
                        else
                        {
                            List<object> new_args = new List<object>();
                            new_args.Add(ids_string);
                            new_args.AddRange(args);

                            sql = string.Format(sql_command, new_args.ToArray());
                        }

                        using (MySqlCommand command = new MySqlCommand(sql, EnsureConnection()))
                        {
                            using (MySqlDataReader reader = command.ExecuteReader())
                            {
                                while (reader.Read())
                                {
                                    Node node = ReadNode(reader);

                                    if (!nodes.ContainsKey(node.Id.Value))
                                    {
                                        nodes.Add(node.Id.Value, node);
                                    }
                                }
                                reader.Close();
                            }
                        }
                    }
                }

                GetNodesTags(EnsureConnection(), nodes);

                return_list = nodes.Values.ToList();
            }

            return return_list;            
        }

        /// <summary>
        /// Returns all the nodes matching the passed ids
        /// </summary>
        /// <param name="ids">The list of ids to search the db for</param>
        /// <returns>Nodes which match the ids given</returns>
        public override IList<Node> GetNodes(IList<long> ids)
        {
            return GetNodesGivenSQL(ids, SELECT_NODES);
        }

        /// <summary>
        /// Returns all the nodes for the given tile range
        /// </summary>
        /// <param name="range">The tile range to match nodes against</param>
        /// <returns>The list of matching nodes from the db</returns>
        public IList<Node> GetNodes(TileRange range)
        {
            var tile_ids = ConstructIdList(range);

            return GetNodesForTiles(tile_ids);
        }

        /// <summary>
        /// Returns all the nodes within the given coordinate box
        /// </summary>
        /// <param name="box">The coordinate box to match against</param>
        /// <returns>The list of matching nodes from the db</returns>
        public IList<Node> GetNodes(GeoCoordinateBox box)
        {
            TileRange tile_range = TileRange.CreateAroundBoundingBox(box, MySQLSchemaTools.DefaultTileZoomLevel);

            var tile_ids = ConstructIdList(tile_range);

            return GetNodesGivenSQL(tile_ids, SELECT_NODES_FROM_TILES_AND_GEO_COORDS,
                                    MySQLSchemaTools.GeoToDB(box.MinLat), MySQLSchemaTools.GeoToDB(box.MaxLat),
                                    MySQLSchemaTools.GeoToDB(box.MinLon), MySQLSchemaTools.GeoToDB(box.MaxLon));
        }

        /// <summary>
        /// Returns all the nodes for the given tiles
        /// </summary>
        /// <param name="tile_ids"></param>
        /// <returns></returns>
        public IList<Node> GetNodesForTiles(IList<long> tile_ids)
        {
            return GetNodesGivenSQL(tile_ids, SELECT_NODES_FROM_TILES);
        }

        /// <summary>
        /// Returns all the nodes for the given tiles
        /// </summary>
        /// <param name="tiles"></param>
        /// <returns></returns>
        public IList<Node> GetNodesForTiles(IList<Tile> tiles)
        {
            var tile_ids = new List<long>();

            foreach (var tile in tiles)
            {
                tile_ids.Add((long)tile.Id);
            }

            return GetNodesForTiles(tile_ids);
        }

        /// <summary>
        /// Reads a node from the MySqlDataReader
        /// </summary>
        /// <param name="reader">The MySqlDataReader to attempt reading the node from</param>
        /// <returns>The node read from the MySqlReader</returns>
        private Node ReadNode(MySqlDataReader reader)
        {
            var node = new Node();

            node.Id = reader.GetInt64(0);
            node.Latitude = MySQLSchemaTools.DBToGeo(reader.GetInt32(1));
            node.Longitude = MySQLSchemaTools.DBToGeo(reader.GetInt32(2));
            node.ChangeSetId = reader.IsDBNull(3) ? null : (long?)reader.GetInt64(3);
            node.Visible = reader.IsDBNull(4) ? null : (bool?)reader.GetBoolean(4);
            node.TimeStamp = reader.IsDBNull(5) ? null : (DateTime?)reader.GetDateTime(5);
            node.Version = reader.IsDBNull(7) ? null : (ulong?)reader.GetInt64(7);
            node.UserName = reader.IsDBNull(8) ? null : reader.GetString(8);
            node.UserId = reader.IsDBNull(9) ? null : (long?)reader.GetInt64(9);

            return node;
        }

        /// <summary>
        /// Gets the tags for a node from a MySQL connection
        /// </summary>
        /// <param name="connection">The MySql connection to attempt reading the node tags from</param>
        /// <param name="node">The node to add the tags to</param>
        private void GetNodeTags(MySqlConnection connection, Node node)
        {
            var sql = string.Format(SELECT_NODE_TAGS, node.Id);

            using (MySqlCommand command = new MySqlCommand(sql, connection))
            {
                using (MySqlDataReader reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        Tag tag = ReadTag(reader);

                        if (node.Tags == null)
                        {
                            node.Tags = new TagsCollection();
                        }

                        if (!node.Tags.ContainsKey(tag.Key))
                        {
                            node.Tags.Add(tag);
                        }
                    }
                }
            }
        }

        /// <summary>
        /// Gets the tags for a list of nodes from a MySQL connection
        /// </summary>
        /// <param name="connection">The MySql connection to attempt reading the node tags from</param>
        /// <param name="id_nodes">The dictionary of ids and nodes to get the tags for</param>
        private void GetNodesTags(MySqlConnection connection, Dictionary<long, Node> id_nodes)
        {
            var node_ids = id_nodes.Keys.ToList();

            if (node_ids.Count > 0)
            {
                for (int idx_1000 = 0; idx_1000 <= node_ids.Count / 1000; idx_1000++)
                {
                    int start_idx = idx_1000 * 1000;
                    int stop_idx = System.Math.Min((idx_1000 + 1) * 1000, node_ids.Count);
                    var ids_string = ConstructIdList(node_ids, start_idx, stop_idx);

                    if (ids_string.Length > 0)
                    {
                        var sql = string.Format(SELECT_NODES_IDS_TAGS, ids_string);

                        using (MySqlCommand command = new MySqlCommand(sql, EnsureConnection()))
                        {
                            using (MySqlDataReader reader = command.ExecuteReader())
                            {
                                while (reader.Read())
                                {
                                    Tuple<long, Tag> node_tag = ReadIdAndTag(reader);

                                    if (id_nodes[node_tag.Item1].Tags == null)
                                    {
                                        id_nodes[node_tag.Item1].Tags = new TagsCollection();
                                    }

                                    if (!id_nodes[node_tag.Item1].Tags.ContainsKey(node_tag.Item2.Key))
                                    {
                                        id_nodes[node_tag.Item1].Tags.Add(node_tag.Item2);
                                    }
                                }
                                reader.Close();
                            }
                        }
                    }
                }
            }
        }

        /// <summary>
        /// Reads a tag from the MySqlDataReader
        /// </summary>
        /// <param name="reader">The MySqlDataReader to attempt reading the tag from</param>
        /// <returns>The tag read from the MySqlReader</returns>
        private Tag ReadTag(MySqlDataReader reader)
        {
            return new Tag(reader.GetString(0), reader.GetString(1));
        }

        /// <summary>
        /// Reads a tag and tag's owner id from the MySqlDataReader
        /// </summary>
        /// <param name="reader">The MySqlDataReader to attempt reading the tag from</param>
        /// <returns>Tuple containing the tag owners id and the tag from the MySqlReader</returns>
        private Tuple<long, Tag> ReadIdAndTag(MySqlDataReader reader)
        {
            return new Tuple<long,Tag>(reader.GetInt64(0), new Tag(reader.GetString(1), reader.GetString(2)));
        }

        /// <summary>
        /// Returns all ways but use the existing nodes to fill the Nodes-lists.
        /// </summary>
        /// <param name="way_ids"></param>
        /// <returns></returns>
        public override IList<Way> GetWays(IList<long> way_ids)
        {         
            var return_list = new List<Way>();
            if (way_ids.Count > 0)
            {
                var ways = new Dictionary<long, Way>();
                for (int idx_1000 = 0; idx_1000 <= way_ids.Count / 1000; idx_1000++)
                {
                    int start_idx = idx_1000 * 1000;
                    int stop_idx = System.Math.Min((idx_1000 + 1) * 1000, way_ids.Count);
                    string ids_string = ConstructIdList(way_ids, start_idx, stop_idx);

                    if (ids_string.Length > 0)
                    {
                        string sql = string.Format(SELECT_WAYS, ids_string);

                        using (MySqlCommand command = new MySqlCommand(sql, EnsureConnection()))
                        {
                            using (MySqlDataReader reader = command.ExecuteReader())
                            {
                                while (reader.Read())
                                {
                                    Way way = ReadWay(reader);

                                    if (!ways.ContainsKey(way.Id.Value))
                                    {
                                        ways.Add(way.Id.Value, way);
                                    }
                                }

                                reader.Close();
                            }
                        }

                        GetWaysTags(EnsureConnection(), ids_string, ways);
                        GetWaysNodes(EnsureConnection(), ids_string, ways);
                    }
                }

                return_list = ways.Values.ToList();
            }

            return return_list;
        }

        /// <summary>
        /// Returns all ways but use the existing nodes to fill the Nodes-lists.
        /// </summary>
        /// <param name="nodes">List of nodes to find ways for</param>
        /// <returns>List of ways that contain the passed nodes</returns>
        public IList<Way> GetWays(IList<Node> nodes)
        {
            if (nodes.Count > 0)
            {
                var way_id_list = new List<long>();

                foreach (Node node in nodes)
                {
                    way_id_list.Add((long)node.Id);
                }

                return GetWaysFor(way_id_list);                
            }

            return new List<Way>();
        }

        /// <summary>
        /// Reads a way from the MySqlDataReader
        /// </summary>
        /// <param name="reader">The MySqlDataReader to attempt reading the way from</param>
        /// <returns>The way read from the MySqlReader</returns>
        private Way ReadWay(MySqlDataReader reader)
        {
            var way = new Way();

            way.Id = reader.GetInt64(0);
            way.ChangeSetId = reader.IsDBNull(1) ? null : (long?)reader.GetInt64(1);
            way.Visible = reader.IsDBNull(2) ? null : (bool?)reader.GetBoolean(2);
            way.TimeStamp = reader.IsDBNull(3) ? null : (DateTime?)reader.GetDateTime(3);
            way.Version = reader.IsDBNull(4) ? null : (ulong?)reader.GetInt64(4);
            way.UserName = reader.IsDBNull(5) ? null : reader.GetString(5);
            way.UserId = reader.IsDBNull(6) ? null : (long?)reader.GetInt64(6);

            return way;
        }

        /// <summary>
        /// Gets the tags for a way from a MySQL connection
        /// </summary>
        /// <param name="connection">The MySql connection to attempt reading the way tags from</param>
        /// <param name="way">The way to add the tags to</param>
        private void GetWayTags(MySqlConnection connection, Way way)
        {
            var sql = string.Format(SELECT_WAY_TAGS, way.Id);

            using (MySqlCommand command = new MySqlCommand(sql, connection))
            {
                using (MySqlDataReader reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        Tag tag = ReadTag(reader);

                        if (way.Tags == null)
                        {
                            way.Tags = new TagsCollection();
                        }

                        if (!way.Tags.ContainsKey(tag.Key))
                        {
                            way.Tags.Add(tag);
                        }
                    }

                    reader.Close();
                }
            }
        }

        /// <summary>
        /// Gets the tags for a list of ways from a MySQL connection
        /// </summary>
        /// <param name="connection">The MySql connection to attempt reading the way tags from</param>
        /// <param name="way_ids">The prepared id string for reading the way ids and tags from</param>
        /// <param name="id_ways">The id way dictionary to place the tags into</param>
        private void GetWaysTags(MySqlConnection connection, string way_ids, Dictionary<long, Way> id_ways)
        {
            if (way_ids.Length > 0)
            {
                var sql = string.Format(SELECT_WAYS_IDS_TAGS, way_ids);

                using (MySqlCommand command = new MySqlCommand(sql, EnsureConnection()))
                {
                    using (MySqlDataReader reader = command.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            Tuple<long, Tag> way_tag = ReadIdAndTag(reader);

                            if (id_ways[way_tag.Item1].Tags == null)
                            {
                                id_ways[way_tag.Item1].Tags = new TagsCollection();
                            }

                            if (!id_ways[way_tag.Item1].Tags.ContainsKey(way_tag.Item2.Key))
                            {
                                id_ways[way_tag.Item1].Tags.Add(way_tag.Item2);
                            }
                        }

                        reader.Close();
                    }
                }
            }             
        }

        /// <summary>
        /// Gets the nodeids for a way from a MySQL connection
        /// </summary>
        /// <param name="connection">The MySql connection to attempt reading the ways nodeids from</param>
        /// <param name="way">The way to add the nodeids to</param>
        private void GetWayNodes(MySqlConnection connection, Way way)
        {
            var sql = string.Format(SELECT_WAY_NODES, way.Id);

            using (MySqlCommand command = new MySqlCommand(sql, connection))
            {
                using (MySqlDataReader reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        long node_id = ReadNodeId(reader);

                        if (way.Nodes == null)
                        {
                            way.Nodes = new List<long>();
                        }

                        way.Nodes.Add(node_id);
                    }

                    reader.Close();
                }
            }
        }

        /// <summary>
        /// Gets the node ids for a list of ways from a MySQL connection
        /// </summary>
        /// <param name="connection">The MySql connection to attempt reading the way node ids from</param>
        /// <param name="way_ids">The prepared id string to get the way and node ids from</param>
        /// <param name="id_ways">The id way dictionary to place the node ids into</param>
        private void GetWaysNodes(MySqlConnection connection, string way_ids, Dictionary<long, Way> id_ways)
        {
            if (way_ids.Length > 0)
            {
                var sql = string.Format(SELECT_WAYS_IDS_NODES, way_ids);

                using (MySqlCommand command = new MySqlCommand(sql, EnsureConnection()))
                {
                    using (MySqlDataReader reader = command.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            var way_id_node_id = ReadWayIdAndNodeId(reader);

                            if (id_ways[way_id_node_id.Item1].Nodes == null)
                            {
                                id_ways[way_id_node_id.Item1].Nodes = new List<long>();
                            }

                            id_ways[way_id_node_id.Item1].Nodes.Add(way_id_node_id.Item2);
                        }

                        reader.Close();
                    }
                }
            }
        }

        /// <summary>
        /// Reads a node id from the MySqlDataReader
        /// </summary>
        /// <param name="reader">The MySqlDataReader to attempt reading the node id from</param>
        /// <returns>The node id read from the MySqlReader</returns>
        private long ReadNodeId(MySqlDataReader reader)
        {
            return reader.GetInt64(0);
        }

        /// <summary>
        /// Reads a way and node id pair from the MySqlDataReader
        /// </summary>
        /// <param name="reader">The MySqlDataReader to attempt reading the node id from</param>
        /// <returns>The node id read from the MySqlReader</returns>
        private Tuple<long, long> ReadWayIdAndNodeId(MySqlDataReader reader)
        {
            return new Tuple<long, long>(reader.GetInt64(0), reader.GetInt64(1));
        }

        /// <summary>
        /// Returns all ways using the given node.
        /// </summary>
        /// <param name="node_id"></param>
        /// <returns></returns>
        public override IList<Way> GetWaysFor(long node_id)
        {
            var node_ids = new List<long>();
            node_ids.Add(node_id);

            return GetWaysFor(node_ids);
        }

        /// <summary>
        /// Returns all ways using any of the given nodes.
        /// </summary>
        /// <param name="node_ids"></param>
        /// <returns></returns>
        public IList<Way> GetWaysFor(List<long> node_ids)
        {            
            if (node_ids.Count > 0)
            {
                var node_ids_string = ConstructIdList(node_ids);
                var sql = string.Format(SELECT_WAYS_FROM_NODES, node_ids_string);
                var way_ids = new List<long>();

                using (MySqlCommand command = new MySqlCommand(sql, EnsureConnection()))
                {
                    using (MySqlDataReader reader = command.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            way_ids.Add(ReadNodeId(reader));
                        }
                    }
                }

                return GetWays(way_ids);
            }

            return new List<Way>();
        }

        /// <summary>
        /// Returns the relations for the given ids.
        /// </summary>
        /// <param name="relation_ids"></param>
        /// <returns></returns>
        public override IList<Relation> GetRelations(IList<long> relation_ids)
        {
            var return_list = new List<Relation>();
            if (relation_ids.Count > 0)
            {
                var relations = new Dictionary<long, Relation>();
                for (int idx_1000 = 0; idx_1000 <= relation_ids.Count / 1000; idx_1000++)
                {
                    int start_idx = idx_1000 * 1000;
                    int stop_idx = System.Math.Min((idx_1000 + 1) * 1000, relation_ids.Count);
                    string ids_string = ConstructIdList(relation_ids, start_idx, stop_idx);

                    if (ids_string.Length > 0)
                    {
                        string sql = string.Format(SELECT_RELATIONS, ids_string);

                        using (MySqlCommand command = new MySqlCommand(sql, EnsureConnection()))
                        {
                            using (MySqlDataReader reader = command.ExecuteReader())
                            {
                                while (reader.Read())
                                {
                                    Relation relation = ReadRelation(reader);

                                    if (!relations.ContainsKey(relation.Id.Value))
                                    {
                                        relations.Add(relation.Id.Value, relation);
                                    }
                                }

                                reader.Close();
                            }
                        }

                        GetRelationsTags(EnsureConnection(), ids_string, relations);
                        GetRelationsMembers(EnsureConnection(), ids_string, relations);
                    }
                }

                return_list = relations.Values.ToList();
            }

            return return_list; 
        }

        /// <summary>
        /// Returns the relations that contain any of the geos
        /// </summary>
        /// <param name="geos">The geometries to be searched against</param>
        /// <returns>A list of relations that contain any of the geos passed</returns>
        public IList<Relation> GetRelationsFor(IList<OsmGeo> geos)
        {
            var relations = new List<Relation>();

            if (geos.Count > 0)
            {
                var relation_ids = new HashSet<long>();

                relations = GetRelationsForBatch(geos);

                // todo : push down into sql fetch stage
                foreach (var relation in relations)
                {
                    relation_ids.Add(relation.Id.Value);
                }

                // recursively add all relations containing above relations as a member
                var remaining_relations = relations;
                do
                {
                    var new_relations = new List<Relation>();

                    // get the relations for each remaining relation
                    var relations_for = GetRelationsForBatch(remaining_relations.Cast<OsmGeo>().ToList());

                    foreach (Relation relation_for in relations_for)
                    {
                        // if we don't already have this relation, we need to search it additionally for relations
                        if (!relation_ids.Contains(relation_for.Id.Value))
                        {
                            // ensure we don't search for this again
                            relation_ids.Add(relation_for.Id.Value);

                            // add to the next searchable list
                            new_relations.Add(relation_for);
                        }
                    }

                    // add to the highest level list
                    Utilities.AddRange(relations, new_relations);

                    // the next set of relations to be searched is the set just found
                    remaining_relations = new_relations;
                } while (remaining_relations.Count > 0); 
            }

            return relations;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="geos"></param>
        private List<Relation> GetRelationsForBatch(IList<OsmGeo> geos)
        {
            var final_relations = new List<Relation>();

            if (geos.Count > 0)
            {
                var nodes = new Dictionary<long, Node>();
                var ways = new Dictionary<long, Way>();
                var relations = new Dictionary<long, Relation>();

                foreach (var geo in geos)
                {
                    if (geo.Type == OsmGeoType.Node)
                    {
                        if (!nodes.ContainsKey(geo.Id.Value))
                        {
                            nodes.Add(geo.Id.Value, geo as Node);
                        }
                    }
                    else if (geo.Type == OsmGeoType.Way)
                    {
                        if (!ways.ContainsKey(geo.Id.Value))
                        {
                            ways.Add(geo.Id.Value, geo as Way);
                        }                        
                    }
                    else if (geo.Type == OsmGeoType.Relation)
                    {
                        if (!relations.ContainsKey(geo.Id.Value))
                        {
                            relations.Add(geo.Id.Value, geo as Relation);
                        }                        
                    }
                }

                var node_ids = ConstructIdList(nodes.Keys);
                var way_ids = ConstructIdList(ways.Keys);
                var relation_ids = ConstructIdList(relations.Keys);

                final_relations.AddRange(GetRelationsFor(node_ids, OsmGeoType.Node));
                final_relations.AddRange(GetRelationsFor(way_ids, OsmGeoType.Way));
                final_relations.AddRange(GetRelationsFor(relation_ids, OsmGeoType.Relation));
            }

            return final_relations;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="ids_string"></param>
        /// <param name="member_type"></param>
        private IList<Relation> GetRelationsFor(string ids_string, OsmGeoType member_type)
        {
            if (ids_string.Length > 0)
            {
                var sql = string.Format(SELECT_RELATIONS_FOR_MEMBER, ids_string, ConvertMemberTypeShort(member_type));
                var relation_ids = new List<long>();

                using (MySqlCommand command = new MySqlCommand(sql, EnsureConnection()))
                {
                    using (MySqlDataReader reader = command.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            relation_ids.Add(ReadNodeId(reader));
                        }
                    }
                }

                return GetRelations(relation_ids);
            }

            return new List<Relation>();
        }

        /// <summary>
        /// Reads a relation from the MySqlDataReader
        /// </summary>
        /// <param name="reader">The MySqlDataReader to attempt reading the relation from</param>
        /// <returns>The relation read from the MySqlReader</returns>
        private Relation ReadRelation(MySqlDataReader reader)
        {
            var relation = new Relation();

            relation.Id = reader.GetInt64(0);
            relation.ChangeSetId = reader.IsDBNull(1) ? null : (long?)reader.GetInt64(1);
            relation.Visible = reader.IsDBNull(2) ? null : (bool?)reader.GetBoolean(2);
            relation.TimeStamp = reader.IsDBNull(3) ? null : (DateTime?)reader.GetDateTime(3);
            relation.Version = reader.IsDBNull(4) ? null : (ulong?)reader.GetInt64(4);
            relation.UserName = reader.IsDBNull(5) ? null : reader.GetString(5);
            relation.UserId = reader.IsDBNull(6) ? null : (long?)reader.GetInt64(6);

            return relation;
        }

        /// <summary>
        /// Gets the tags for a relation from a MySQL connection
        /// </summary>
        /// <param name="connection">The MySql connection to attempt reading the relation tags from</param>
        /// <param name="relation">The relation to add the tags to</param>
        private void GetRelationTags(MySqlConnection connection, Relation relation)
        {
            var sql = string.Format(SELECT_RELATION_TAGS, relation.Id);

            using (MySqlCommand command = new MySqlCommand(sql, connection))
            {
                using (MySqlDataReader reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        Tag tag = ReadTag(reader);

                        if (relation.Tags == null)
                        {
                            relation.Tags = new TagsCollection();
                        }

                        if (!relation.Tags.ContainsKey(tag.Key))
                        {
                            relation.Tags.Add(tag);
                        }
                    }

                    reader.Close();
                }
            }
        }

        /// <summary>
        /// Gets tags for relations based on a pre-processed relation list string
        /// </summary>
        /// <param name="connection">The MySql connection to attempt reading the relation ids and tags from</param>
        /// <param name="id_relations">The prepared id string to fetch the relation ids and tags from</param>
        /// <param name="relation_ids">The relation id relation dictionary to add the tags to</param>
        private void GetRelationsTags(MySqlConnection connection, string relation_ids, Dictionary<long, Relation> id_relations)
        {
            if (relation_ids.Length > 0)
            {
                var sql = string.Format(SELECT_RELATIONS_IDS_TAGS, relation_ids);

                using (MySqlCommand command = new MySqlCommand(sql, EnsureConnection()))
                {
                    using (MySqlDataReader reader = command.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            var id_tag = ReadIdAndTag(reader);

                            if (id_relations[id_tag.Item1].Tags == null)
                            {
                                id_relations[id_tag.Item1].Tags = new TagsCollection();
                            }

                            if (!id_relations[id_tag.Item1].Tags.ContainsKey(id_tag.Item2.Key))
                            {
                                id_relations[id_tag.Item1].Tags.Add(id_tag.Item2);
                            }
                        }

                        reader.Close();
                    }
                }
            }
        }

        /// <summary>
        /// Gets the members for a relation from a MySQL connection
        /// </summary>
        /// <param name="connection">The MySql connection to attempt reading the relation members from</param>
        /// <param name="relation">The relation to add the members to</param>
        private void GetRelationMembers(MySqlConnection connection, Relation relation)
        {
            var sql = string.Format(SELECT_RELATION_MEMBERS, relation.Id);

            using (MySqlCommand command = new MySqlCommand(sql, connection))
            {
                using (MySqlDataReader reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        var relation_member = ReadRelationMember(reader);

                        if (relation.Members == null)
                        {
                            relation.Members = new List<RelationMember>();
                        }

                        relation.Members.Add(relation_member);
                    }
                }
            }
        }

        /// <summary>
        /// Gets members for relations based on a pre-processed relation list string
        /// </summary>
        /// <param name="connection">The MySql connection to attempt reading the relation ids and members from</param>
        /// <param name="id_relations">The prepared id string to fetch the relation ids and members from</param>
        /// <param name="relation_ids">The relation id relation dictionary to add the members to</param>
        private void GetRelationsMembers(MySqlConnection connection, string relation_ids, Dictionary<long, Relation> id_relations)
        {
            if (relation_ids.Length > 0)
            {
                var sql = string.Format(SELECT_RELATIONS_IDS_MEMBERS, relation_ids);

                using (MySqlCommand command = new MySqlCommand(sql, EnsureConnection()))
                {
                    using (MySqlDataReader reader = command.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            var relation_id_member = ReadRelationIdAndMember(reader);

                            if (id_relations[relation_id_member.Item1].Members == null)
                            {
                                id_relations[relation_id_member.Item1].Members = new List<RelationMember>();
                            }

                            id_relations[relation_id_member.Item1].Members.Add(relation_id_member.Item2);
                        }

                        reader.Close();
                    }
                }
            }
        }

        /// <summary>
        /// Reads a relation member from the MySqlDataReader
        /// </summary>
        /// <param name="reader">The MySqlDataReader to attempt reading the relation member from</param>
        /// <returns>The relation member read from the MySqlReader</returns>
        private RelationMember ReadRelationMember(MySqlDataReader reader)
        {
            var relation_member = new RelationMember();
            
            relation_member.MemberType = ConvertMemberType(reader.GetInt16(0));
            relation_member.MemberId = reader.GetInt64(1);
            relation_member.MemberRole = reader.GetString(2);

            return relation_member;
        }

        /// <summary>
        /// Reads a relation id and relation member from the MySqlDataReader
        /// </summary>
        /// <param name="reader">The MySqlDataReader to attempt reading the relation id and member from</param>
        /// <returns>The relation member and relation id read from the MySqlReader</returns>
        private Tuple<long,RelationMember> ReadRelationIdAndMember(MySqlDataReader reader)
        {
            var relation_member = new RelationMember();

            relation_member.MemberType = ConvertMemberType(reader.GetInt16(1));
            relation_member.MemberId = reader.GetInt64(2);
            relation_member.MemberRole = reader.GetString(3);

            return new Tuple<long,RelationMember>(reader.GetInt64(0), relation_member);
        }

        /// <summary>
        /// Returns all relations for the given objects.
        /// </summary>
        /// <param name="type"></param>
        /// <param name="id"></param>
        /// <returns></returns>
        public override IList<Relation> GetRelationsFor(OsmGeoType type, long id)
        {
            var sql = string.Format(SELECT_RELATIONS_FOR_MEMBER, id, ConvertMemberTypeShort(type));
            var relation_ids = new List<long>();

            using (MySqlCommand command = new MySqlCommand(sql, EnsureConnection()))
            {
                using (MySqlDataReader reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        relation_ids.Add(ReadNodeId(reader));
                    }
                }
            }

            return GetRelations(relation_ids);
        }

        /// <summary>
        /// Returns all data within the given bounding box and filtered by the given filter.
        /// </summary>
        /// <param name="box">The bounding box to search within</param>
        /// <param name="filter">Filtering options for the results</param>
        /// <returns>Matching OsmGeos</returns>
        public override IList<OsmGeo> Get(GeoCoordinateBox box, Filter filter)
        {            
            var geos = new List<OsmGeo>();

            var nodes = GetNodes(box);
            geos.AddRange(nodes);

            var ways = GetWays(nodes);
            geos.AddRange(ways);

            var relations = GetRelationsFor(geos);
            geos.AddRange(relations);

            return geos;
        }

        /// <summary>
        /// Gets all geometries in the given list of tiles
        /// </summary>
        /// <param name="tiles">List of tiles to fetch geometries from</param>
        /// <param name="filter">Filtering options for the results</param>
        /// <returns>Matching OsmGeos</returns>
        public IList<OsmGeo> Get(IList<Tile> tiles, Filter filter)
        {
            var geos = new List<OsmGeo>();

            var nodes = GetNodesForTiles(tiles);
            geos.AddRange(nodes);

            var ways = GetWays(nodes);
            geos.AddRange(ways);

            var relations = GetRelationsFor(geos);
            geos.AddRange(relations);

            return geos;
        }

        /// <summary>
        /// Gets all geometries in the tile
        /// </summary>
        /// <param name="tile">The tile to fetch geometries from</param>
        /// <param name="filter">Filtering options for the results</param>
        /// <returns>Matching OsmGeos</returns>
        public IList<OsmGeo> Get(Tile tile, Filter filter)
        {
            var tiles = new List<Tile>();

            tiles.Add(tile);

            return Get(tiles, filter);
        }

        /// <summary>
        /// Returns all data within the given tile
        /// </summary>
        /// <param name="tile">The tile to fetch geometries from</param>
        /// <param name="filter">Filtering options for the results</param>
        /// <returns>An OsmGeoCollection object containing the data within the given tile</returns>
        public override OsmGeoCollection GetCollection(Tile tile, Filter filter)
        {
            var tiles = new List<Tile>();

            tiles.Add(tile);

            return GetCollection(tiles, filter);
        }

        /// <summary>
        /// Returns all data within the given tiles
        /// </summary>
        /// <param name="tiles">The tiles to fetch geometries from</param>
        /// <param name="filter">Filtering options for the results</param>
        /// <returns>An OsmGeoCollection object containing the data within the given tile</returns>
        public override OsmGeoCollection GetCollection(IList<Tile> tiles, Filter filter)
        {
            var geos = Get(tiles, filter);
            var collection = new OsmGeoCollection();

            foreach (var geo in geos)
            {
                if (geo.Type == OsmGeoType.Node)
                {
                    var node = geo as Node;
                    collection.Nodes.Add(node.Id.Value, node);
                }
                else if (geo.Type == OsmGeoType.Way)
                {
                    var way = geo as Way;
                    collection.Ways.Add(way.Id.Value, way);
                }
                else if (geo.Type == OsmGeoType.Relation)
                {
                    var relation = geo as Relation;

                    if (!collection.Relations.ContainsKey(relation.Id.Value))
                    {
                        collection.Relations.Add(relation.Id.Value, relation);
                    }
                }
            }

            return collection;
        }

        /// <summary>
        /// Provides a copy of the object that is safe to
        /// read from at the same time as the source
        /// </summary>
        public override IDataSourceReadOnly ConcurrentCopy()
        {
            return new MySQLDataSource(_connection_string);
        }

        #endregion IDataSourceReadOnly Members

        /// <summary>
        /// Closes this datasource.
        /// </summary>
        public void Close()
        {
            if (_connection_owner)
            {
                if (_connection != null && !Utilities.IsNullOrWhiteSpace(_connection_string))
                {
                    // connection exists and was created here; close it here!
                    _connection.Close();
                    _connection.Dispose();
                    _connection = null;
                }
            }
        }

        #region IDisposable Members

        /// <summary>
        /// Diposes the resources used in this datasource.
        /// </summary>
        public void Dispose()
        {
            _connection.Close();
            _connection.Dispose();
            _connection = null;
        }

        #endregion IDisposable Members
    }
}
