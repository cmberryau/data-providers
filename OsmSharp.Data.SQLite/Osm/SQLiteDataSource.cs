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

using OsmSharp.Collections.Tags;
using OsmSharp.Math.Geo;
using OsmSharp.Osm;
using OsmSharp.Osm.Cache;
using OsmSharp.Osm.Collections;
using OsmSharp.Osm.Data;
using OsmSharp.Osm.Filters;
using OsmSharp.Osm.Tiles;
using System;
using System.Collections.Generic;
using System.Data;
using Mono.Data.Sqlite;
using System.Linq;
using System.Text;

namespace OsmSharp.Data.SQLite.Osm
{
    /// <summary>
    /// An SQLite data source.
    /// </summary>
	public class SQLiteDataSource : DataSourceReadOnlyBase, IDisposable
    {
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
                return "SQLite data source";
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
        /// The default zoom level that this data source reads at
        /// </summary>
        public override int DefaultZoomLevel
        {
            get
            {

                return SQLiteSchemaTools.DefaultTileZoomLevel;
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

        private SqliteConnection _connection;
        private readonly string _connection_string;
        private bool _connection_owner;
        private readonly bool _create_and_detect_schema;
        private readonly Guid _id;

        private readonly OsmDataCache _geo_cache;
        private readonly ConcurrentTagsCollectionCache _tags_cache;

        #region command strings

        private const string SELECT_NODES_STUB = @"SELECT " +
                                                     @"node.id, " +
                                                     @"node.latitude, " +
                                                     @"node.longitude, " +
                                                     @"node.name " +
                                                 @"FROM " +
                                                     @"node ";

        private const string SELECT_NODES = SELECT_NODES_STUB + @"WHERE " +
                                                                    @"node.id " +
                                                                @"IN ({0});";

        private const string SELECT_NODES_FROM_TILES = SELECT_NODES_STUB + @"WHERE " +
                                                                               @"node.tile " +
                                                                           @"IN ({0});";

        private const string SELECT_NODES_FROM_TILES_AND_GEO_COORDS = SELECT_NODES_STUB +
                                                                      @"WHERE node.tile IN ({0})" +
                                                                      @"AND (node.latitude >= {1} " +
                                                                      @"AND node.latitude < {2} " +
                                                                      @"AND node.longitude >= {3} " +
                                                                      @"AND node.longitude < {4});";

        private const string SELECT_TAGS = @"SELECT " +
                                               @"{0}_tags.tag_key, " +
                                               @"{0}_tags.value " +
                                           @"FROM " +
                                               @"{0}_tags " +
                                           @"WHERE {0}_tags.id IN ({1});";

        private const string SELECT_GEO_TAG_IDS = @"SELECT " +
                                                      @"{0}.id, " +
                                                      @"{0}.tags_id " +
                                                  @"FROM " +
                                                      @"{0} " +
                                                  @"WHERE " +
                                                      @"{0}.id " +
                                                  @"IN " +
                                                      @"({1}) " +
                                                  @"AND " +
                                                      @"{0}.tags_id " +
                                                  @"IS " +
                                                      @"NOT NULL;";

        private const string SELECT_TAGS_ID = @"SELECT " +
                                                   @"{0}_tags.id, " +
                                                   @"{0}_tags.tag_key, " +
                                                   @"{0}_tags.value " +
                                               @"FROM " +
                                                   @"{0}_tags " +
                                               @"WHERE {0}_tags.id IN ({1});";

        private const string SELECT_WAYS = @"SELECT " +
                                                @"way.id, " +
                                                @"way.name " +
                                            @"FROM " +
                                                @"way " +
                                            @"WHERE way.id IN ({0});";

        private const string SELECT_WAY_NODE_IDS = @"SELECT " +
                                                        @"way_nodes.node_id " +
                                                   @"FROM " +
                                                        @"way_nodes " +
                                                   @"WHERE way_nodes.way_id IN ({0}) " +
                                                   @"ORDER BY way_nodes.sequence_id;";

        private const string SELECT_WAYS_IDS_NODES = @"SELECT " +
                                                         @"way_nodes.way_id, " +
                                                         @"way_nodes.node_id " +
                                                     @"FROM " +
                                                         @"way_nodes " +
                                                     @"WHERE way_nodes.way_id IN ({0}) " +
                                                     @"ORDER BY way_nodes.sequence_id;";

        private const string SELECT_WAYS_FROM_NODES = @"SELECT " +
                                                          @"way_nodes.way_id " +
                                                      @"FROM " +
                                                          @"way_nodes " +
                                                      @"WHERE way_nodes.node_id IN ({0});";

        private const string SELECT_RELATIONS = @"SELECT " +
                                                    @"relation.id, " +
                                                    @"relation.name " +
                                                @"FROM " +
                                                    @"relation " +
                                                @"WHERE relation.id IN ({0});";

        private const string SELECT_RELATION_MEMBERS = @"SELECT " +
                                                           @"relation_members.member_type, " +
                                                           @"relation_members.member_id, " +
                                                           @"relation_members.member_role " +
                                                       @"FROM " +
                                                           @"relation_members " +
                                                       @"WHERE relation_members.relation_id IN ({0}) " +
                                                       @"ORDER BY relation_members.sequence_id;";

        private const string SELECT_RELATIONS_IDS_MEMBERS = @"SELECT " +
                                                                @"relation_members.relation_id, " +
                                                                @"relation_members.member_type, " +
                                                                @"relation_members.member_id, " +
                                                                @"relation_members.member_role " +
                                                            @"FROM " +
                                                                @"relation_members " +
                                                            @"WHERE relation_members.relation_id IN ({0}) " +
                                                            @"ORDER BY relation_members.sequence_id;";

        private const string SELECT_RELATIONS_FOR_MEMBER = @"SELECT " +
                                                               @"relation_members.relation_id " +
                                                           @"FROM " +
                                                               @"relation_members " +
                                                           @"WHERE relation_members.member_id IN ({0}) " +
                                                           @"AND relation_members.member_type = {1};";

        private const string SELECT_GEO_IDS_GIVEN_TAGS = @"SELECT " +
                                                            @"{0}.id " +
                                                        @"FROM " +
                                                            @"{0} " +
                                                        @"WHERE " +
                                                            @"{0}.tags_id " +
                                                        @"IN " +
                                                            @"( " +
                                                                @"SELECT " +
                                                                    @"{0}_tags.id " +
                                                                @"FROM " +
                                                                    @"{0}_tags " +
                                                                @"WHERE " +
                                                                    @"{0}_tags.tag_key " +
                                                                @"IS " +
                                                                    @"'{1}' " +
                                                                @"AND " +
                                                                    @"{0}_tags.value " +
                                                                @"IN " +
                                                                @"( " +
                                                                    @"{2} " +
                                                                @") " +
                                                            @");";

        private const string SELECT_UNIQUE_TAGS_FOR_TYPE = @"SELECT " +
                                                             @"{0}_tags.id, {0}_tags.tag_key, {0}_tags.value " +
                                                           @"FROM " +
                                                             @"{0}_tags;";

        private const string SELECT_UNIQUE_TAGS_FOR_TYPE_GIVEN_KEYS = @"SELECT " +
                                                                        @"{0}_tags.id, {0}_tags.tag_key, {0}_tags.value " +
                                                                      @"FROM " +
                                                                        @"{0}_tags " +
                                                                      @"WHERE " +
                                                                        @"{0}_tags.tag_key " +
                                                                      @"IN " +
                                                                        @"( " +
                                                                          @"{1} " +
                                                                        @");";

        private const string UNION = @"UNION";

        #endregion command strings

        /// <summary>
        /// The version of Sqlite that is loaded into the process
        /// </summary>
        public static string SqliteVersion
        {
            get
            {
                var version = "Unknown";
                // create an empty in-memory db
                var connection_string = SQLiteSchemaTools.BuildConnectionString(true);
                using (var connection = new SqliteConnection(connection_string))
                {
                    connection.Open();

                    if (connection.State == ConnectionState.Open)
                    {
                        using (var command = new SqliteCommand(@"SELECT sqlite_version();", connection))
                        {
                            using (var reader = command.ExecuteReader())
                            {
                                while (reader.Read())
                                {
                                    var fetched = reader.GetString(0);

                                    if (!fetched.IsNullOrWhiteSpace())
                                    {
                                        version = fetched;
                                    }
                                }
                            }
                        }
                    }
                }


                return version;
            }
        }

        /// <summary>
        /// Creates a new SQLite data source
        /// </summary>
        /// <param name="connection_string">The connection string for the SQLite db</param>
        /// <param name="create_schema">Do the db schema and tables need to be created?</param>
        public SQLiteDataSource(string connection_string, bool create_schema = false)
        {
            _connection_string = connection_string;
            _create_and_detect_schema = create_schema;

            _connection = EnsureConnection();

            _geo_cache = new ConcurrentOsmDataCacheMemory();
            _tags_cache = new ConcurrentTagsCollectionCache();
            _id = Guid.NewGuid();
        }

        /// <summary>
        /// Creates a new SQLite source
        /// </summary>
        /// <param name="in_memory">Is the DB in memory? (default is false)</param>
        /// <param name="path">The path to the DB, or its descriptor in memory (if any)</param>
        /// <param name="password">The DB password (if any)</param>
        /// <param name="create_schema">Do the db tables need to be created?</param>
        public SQLiteDataSource(bool in_memory = false, string path = null,
                                string password = null, bool create_schema = false)
        {
            // create the connection string
            _connection_string = SQLiteSchemaTools.BuildConnectionString(in_memory, path, password);
            _create_and_detect_schema = create_schema;
            _connection = EnsureConnection();

            _geo_cache = new ConcurrentOsmDataCacheMemory();
            _tags_cache = new ConcurrentTagsCollectionCache();
            _id = Guid.NewGuid();
        }

        /// <summary>
        /// Creates a new SQLite data source
        /// </summary>
        /// <param name="connection">The SQLite connection</param>
        /// <param name="create_schema">Do the db schema and tables need to be created?</param>
        public SQLiteDataSource(SqliteConnection connection, bool create_schema = false)
        {
            _connection = connection;
            _create_and_detect_schema = create_schema;
            _connection_string = connection.ConnectionString;
            // validate the connection
            _connection = EnsureConnection();

            _geo_cache = new ConcurrentOsmDataCacheMemory();
            _tags_cache = new ConcurrentTagsCollectionCache();
            _id = Guid.NewGuid();
        }

        private SQLiteDataSource(string connection_string, OsmDataCache geo_cache,
            ConcurrentTagsCollectionCache tags_cache)
        {
            _connection_string = connection_string;
            _connection = EnsureConnection();

            _tags_cache = tags_cache;
            _geo_cache = geo_cache;
            _id = Guid.NewGuid();
        }

        /// <summary>
        /// Provides a copy of the object that is safe to be read at the same time
        /// </summary>
        public override IDataSourceReadOnly ConcurrentCopy()
        {
            return new SQLiteDataSource(_connection_string, _geo_cache, _tags_cache);
        }

        #region public node queries

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
            var tile_ids = SchemaTools.ConstructIdList(range);

            return GetNodesForTiles(tile_ids);
        }

        /// <summary>
        /// Returns all the nodes within the given coordinate box
        /// </summary>
        /// <param name="box">The coordinate box to match against</param>
        /// <returns>The list of matching nodes from the db</returns>
        public IList<Node> GetNodes(GeoCoordinateBox box)
        {
            var tile_range = TileRange.CreateAroundBoundingBox(box, SQLiteSchemaTools.DefaultTileZoomLevel);

            var tile_ids = SchemaTools.ConstructIdList(tile_range);

            return GetNodesGivenSQL(tile_ids, SELECT_NODES_FROM_TILES_AND_GEO_COORDS,
                                    SQLiteSchemaTools.GeoToDB(box.MinLat), SQLiteSchemaTools.GeoToDB(box.MaxLat),
                                    SQLiteSchemaTools.GeoToDB(box.MinLon), SQLiteSchemaTools.GeoToDB(box.MaxLon));
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
        /// Returns the node ids for the given ways
        /// </summary>
        public IList<Node> GetNodesForWayIds(IList<long> way_ids)
        {
            // fetch the node ids first
            var node_ids = GetIds(way_ids, SELECT_WAY_NODE_IDS);

            return GetNodesGivenSQL(node_ids, SELECT_NODES);
        }

        #endregion public node queries

        #region public way queries

        /// <summary>
        /// Returns all ways but use the existing nodes to fill the Nodes-lists.
        /// </summary>
        /// <param name="way_ids"></param>
        /// <returns></returns>
        public override IList<Way> GetWays(IList<long> way_ids)
        {
            var return_list = _geo_cache.GetWaysList(way_ids, out way_ids);

            if (way_ids.Count > 0)
            {
                var ways = new Dictionary<long, Way>();
                for (var index = 0; index <= way_ids.Count / 1000; index++)
                {
                    var start = index * 1000;
                    var end = System.Math.Min((index + 1) * 1000, way_ids.Count);
                    var ids = SchemaTools.ConstructIdList(way_ids, start, end);

                    if (ids.Length > 0)
                    {
                        var sql = string.Format(SELECT_WAYS, ids);

                        using (var command = new SqliteCommand(sql, EnsureConnection()))
                        {
                            using (var reader = command.ExecuteReader())
                            {
                                while (reader.Read())
                                {
                                    var way = ReadWay(reader);

                                    if (!ways.ContainsKey(way.Id.Value))
                                    {
                                        ways.Add(way.Id.Value, way);
                                    }
                                }

                                reader.Close();
                            }
                        }

                        GetTags(EnsureConnection(), ways);
                        GetWaysNodes(EnsureConnection(), ids, ways);
                    }
                }

                return_list.AddRange(ways.Values.ToList());
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
                var node_id_list = new List<long>();

                foreach (Node node in nodes)
                {
                    node_id_list.Add((long)node.Id);
                }

                return GetWaysFor(node_id_list);
            }

            return new List<Way>();
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
                var node_ids_string = SchemaTools.ConstructIdList(node_ids);
                var sql = string.Format(SELECT_WAYS_FROM_NODES, node_ids_string);
                var way_ids = new List<long>();

                using (var command = new SqliteCommand(sql, EnsureConnection()))
                {
                    using (var reader = command.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            way_ids.Add(ReadOneInt64(reader));
                        }
                    }
                }

                return GetWays(way_ids);
            }

            return new List<Way>();
        }

        #endregion public way queries

        #region public relation queries

        /// <summary>
        /// Returns the relations for the given ids.
        /// </summary>
        /// <param name="relation_ids"></param>
        /// <returns></returns>
        public override IList<Relation> GetRelations(IList<long> relation_ids)
        {
            var return_list = _geo_cache.GetRelationsList(relation_ids, out relation_ids);

            if (relation_ids.Count > 0)
            {
                var relations = new Dictionary<long, Relation>();
                for (var index = 0; index <= relation_ids.Count / 1000; index++)
                {
                    var start = index * 1000;
                    var end = System.Math.Min((index + 1) * 1000, relation_ids.Count);
                    var ids = SchemaTools.ConstructIdList(relation_ids, start, end);

                    if (ids.Length > 0)
                    {
                        var sql = string.Format(SELECT_RELATIONS, ids);
                        using (var command = new SqliteCommand(sql, EnsureConnection()))
                        {
                            using (var reader = command.ExecuteReader())
                            {
                                while (reader.Read())
                                {
                                    var relation = ReadRelation(reader);

                                    if (!relations.ContainsKey(relation.Id.Value))
                                    {
                                        relations.Add(relation.Id.Value, relation);
                                    }
                                }

                                reader.Close();
                            }
                        }

                        GetTags(EnsureConnection(), relations);
                        GetRelationsMembers(EnsureConnection(), ids, relations);
                    }
                }

                return_list.AddRange(relations.Values.ToList());
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

                    foreach (var relation_for in relations_for)
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
        /// Returns all relations for the given objects.
        /// </summary>
        /// <param name="type"></param>
        /// <param name="id"></param>
        /// <returns></returns>
        public override IList<Relation> GetRelationsFor(OsmGeoType type, long id)
        {
            var sql = string.Format(SELECT_RELATIONS_FOR_MEMBER, id, SchemaTools.ConvertMemberTypeShort(type));
            var relation_ids = new List<long>();

            using (var command = new SqliteCommand(sql, EnsureConnection()))
            {
                using (var reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        relation_ids.Add(ReadOneInt64(reader));
                    }
                }
            }

            return GetRelations(relation_ids);
        }

        #endregion public relation queries

        #region public combined queries

        /// <summary>
        /// Returns all data within the given bounding box and filtered by the given filter.
        /// </summary>
        /// <param name="box">The bounding box to search within</param>
        /// <param name="filter">Filtering options for the results</param>
        /// <returns>Matching OsmGeos</returns>
        public override IList<OsmGeo> Get(GeoCoordinateBox box, Filter filter = null)
        {
            var geos = new List<OsmGeo>();

            var nodes = GetNodes(box);
            geos.AddRange(nodes);

            var ways = GetWays(nodes);
            geos.AddRange(ways);

            var relations = GetRelationsFor(geos);
            geos.AddRange(relations);

            var filteredGeos = new List<OsmGeo>();

            if (filter != null)
            {
                foreach (var geo in geos)
                {
                    if (filter.Evaluate(geo))
                    {
                        filteredGeos.Add(geo);
                    }
                }

                geos = filteredGeos;
            }

            return geos;
        }

        /// <summary>
        /// Gets all geometries in the given list of tiles
        /// </summary>
        /// <param name="tiles">List of tiles to fetch geometries from</param>
        /// <param name="filter">Filtering options for the results</param>
        /// <returns>Matching OsmGeos</returns>
        public IList<OsmGeo> Get(IList<Tile> tiles, Filter filter = null)
        {
            var geos = new List<OsmGeo>();

            var nodes = GetNodesForTiles(tiles);
            geos.AddRange(nodes);
            _geo_cache.AddNodes(nodes);

            var ways = GetWays(nodes);
            geos.AddRange(ways);
            _geo_cache.AddWays(ways);

            var relations = GetRelationsFor(geos);
            geos.AddRange(relations);
            _geo_cache.AddRelations(relations);

            var filteredGeos = new List<OsmGeo>();

            if (filter != null)
            {
                foreach (var geo in geos)
                {
                    if (filter.Evaluate(geo))
                    {
                        filteredGeos.Add(geo);
                    }
                }

                geos = filteredGeos;
            }

            return geos;
        }

        /// <summary>
        /// Gets all geometries in the tile
        /// </summary>
        /// <param name="tile">The tile to fetch geometries from</param>
        /// <param name="filter">Filtering options for the results</param>
        /// <returns>Matching OsmGeos</returns>
        public IList<OsmGeo> Get(Tile tile, Filter filter = null)
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
        public override OsmGeoCollection GetCollection(Tile tile, Filter filter = null)
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
        public override OsmGeoCollection GetCollection(IList<Tile> tiles, Filter filter = null)
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
        /// Returns all the objects in this dataset that evaluate the filter to true.
        /// </summary>
        /// <param name="box"></param>
        /// <param name="filter"></param>
        /// <returns></returns>
        public OsmGeoCollection GetCollection(GeoCoordinateBox box, Filter filter = null)
        {
            var geos = Get(box, filter);
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
        /// Returns the unique tags for the given geo type
        /// </summary>
        /// <param name="type">The geo type</param>
        /// <param name="keys">The key filter, only return tag combinations with these keys</param>
        public override HashSet<TagsCollectionBase> UniqueTags(OsmGeoType type, List<string> keys = null)
        {
            if (keys != null)
            {
                if (keys.Count <= 0)
                {
                    keys = null;
                }
            }

            string sql;

            if (keys == null)
            {
                sql = string.Format(SELECT_UNIQUE_TAGS_FOR_TYPE, type.ToString().ToLower());
            }
            else
            {
                sql = string.Format(SELECT_UNIQUE_TAGS_FOR_TYPE_GIVEN_KEYS,
                    type.ToString().ToLower(), keys.CommaSeperatedString());
            }

            Dictionary<int, TagsCollectionBase> uniques;

            using (var command = new SqliteCommand(sql, _connection))
            {
                using (var reader = command.ExecuteReader())
                {
                    uniques = new Dictionary<int, TagsCollectionBase>();

                    while (reader.Read())
                    {
                        var tag = ReadInt32AndTag(reader);

                        if (!uniques.ContainsKey(tag.Item1))
                        {
                            uniques.Add(tag.Item1, new TagsCollection());
                        }

                        uniques[tag.Item1].Add(tag.Item2);
                    }
                }
            }

            var result = new HashSet<TagsCollectionBase>();

            foreach (var collection in uniques)
            {
                result.Add(collection.Value);
            }

            return result;
        }

        /// <summary>
        /// Returns the unique tags for the given geo type
        /// </summary>
        /// <param name="type">The geo type</param>
        /// <param name="key">The key filter, only return tag combinations with this key</param>
        public override HashSet<TagsCollectionBase> UniqueTags(OsmGeoType type, string key)
        {
            return UniqueTags(type, new List<string>() { key });
        }

        /// <summary>
        /// Returns all ways matching the tag passed
        /// </summary>
        public override OsmGeoCollection GetGeosGivenTag(OsmGeoType type, string tag, List<string> values)
        {
            var sql_command = string.Format(SELECT_GEO_IDS_GIVEN_TAGS, type.ToString().ToLower(),
                tag, values.CommaSeperatedString());
            var way_ids = GetWayIdsGivenSQL(sql_command);
            var ways = GetWays(way_ids);

            var result = new OsmGeoCollection();

            foreach (var way in ways)
            {
                result.Ways.Add(way.Id.Value, way);
            }

            var nodes = GetNodesForWayIds(way_ids);

            foreach (var node in nodes)
            {
                result.Nodes.Add(node.Id.Value, node);
            }

            return result;
        }

        /// <summary>
        /// Returns all ways matching the tags passed
        /// </summary>
        public override OsmGeoCollection GetGeosGivenTags(OsmGeoType type, Dictionary<string, List<string>> tags)
        {
            var sb = new StringBuilder();
            int index = 0;

            foreach (var tag in tags)
            {
                if (index > 0)
                {
                    sb.Append(UNION);
                }

                sb.Append(string.Format(SELECT_GEO_IDS_GIVEN_TAGS, type.ToString().ToLower(),
                    tag.Key, tag.Value.CommaSeperatedString()));
                index++;
            }

            var way_ids = GetWayIdsGivenSQL(sb.ToString());
            var ways = GetWays(way_ids);

            var result = new OsmGeoCollection();

            foreach (var way in ways)
            {
                result.Ways.Add(way.Id.Value, way);
            }

            var nodes = GetNodesForWayIds(way_ids);

            foreach (var node in nodes)
            {
                result.Nodes.Add(node.Id.Value, node);
            }

            return result;
        }

        #endregion public combined queries

        /// <summary>
        /// Closes this sqlite data source
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

        /// <summary>
        /// Diposes the resources used in this sqlite data source
        /// </summary>
        public void Dispose()
        {
            Close();
        }

        private SqliteConnection EnsureConnection()
        {
            if (_connection == null)
            {
                _connection = new SqliteConnection(_connection_string);
                _connection_owner = true;
            }

            if (_connection.State != System.Data.ConnectionState.Open)
            {
                _connection.Open();
            }

            if (_create_and_detect_schema)
            {   // attempts to create and detect tables
                SQLiteSchemaTools.CreateAndDetect(_connection);
            }

            return _connection;
        }

        #region private id queries

        private IList<long> GetIds(IList<long> ids, string sql_command, params object[] args)
        {
            var return_list = new List<long>();

            if (ids.Count > 0)
            {
                for (var index = 0; index <= ids.Count / 1000; index++)
                {
                    var start = index * 1000;
                    var end = System.Math.Min((index + 1) * 1000, ids.Count);
                    var id_list = SchemaTools.ConstructIdList(ids, start, end);

                    if (id_list.Length > 0)
                    {
                        string sql;

                        if (args == null)
                        {
                            sql = string.Format(sql_command, id_list);
                        }
                        else if (args.Length == 0)
                        {
                            sql = string.Format(sql_command, id_list);
                        }
                        else
                        {
                            var new_args = new List<object>();
                            new_args.Add(id_list);
                            new_args.AddRange(args);

                            sql = string.Format(sql_command, new_args.ToArray());
                        }

                        using (var command = new SqliteCommand(sql, EnsureConnection()))
                        {
                            using (var reader = command.ExecuteReader())
                            {
                                while (reader.Read())
                                {
                                    var id = ReadOneInt64(reader);
                                    return_list.Add(id);
                                }

                                reader.Close();
                            }
                        }
                    }
                }
            }

            return return_list;
        }

        private long ReadOneInt64(SqliteDataReader reader)
        {
            return reader.GetInt64(0);
        }

        private Tuple<long, long> ReadTwoInt64s(SqliteDataReader reader)
        {
            return new Tuple<long, long>(reader.GetInt64(0), reader.GetInt64(1));
        }

        private Tuple<long, int> ReadOneInt64OneInt32(SqliteDataReader reader)
        {
            return new Tuple<long, int>(reader.GetInt64(0), reader.GetInt32(1));
        }

        #endregion private id queries

        #region private tag queries

        private void GetTags(SqliteConnection connection, Dictionary<long, Node> id_nodes)
        {
            var geo_list = new Dictionary<long, OsmGeo>();

            foreach (var node in id_nodes)
            {
                geo_list.Add(node.Key, node.Value);
            }

            GetTags(connection, geo_list, OsmGeoType.Node);
        }

        private void GetTags(SqliteConnection connection, Dictionary<long, Way> id_ways)
        {
            var geo_list = new Dictionary<long, OsmGeo>();

            foreach (var way in id_ways)
            {
                geo_list.Add(way.Key, way.Value);
            }

            GetTags(connection, geo_list, OsmGeoType.Way);
        }

        private void GetTags(SqliteConnection connection, Dictionary<long, Relation> id_relations)
        {
            var geo_list = new Dictionary<long, OsmGeo>();

            foreach (var relation in id_relations)
            {
                geo_list.Add(relation.Key, relation.Value);
            }

            GetTags(connection, geo_list, OsmGeoType.Relation);
        }

        private void GetTags(SqliteConnection connection, Dictionary<long, OsmGeo> id_geos,
            OsmGeoType type)
        {
            var geo_ids = id_geos.Keys.ToList();

            if (geo_ids.Count > 0)
            {
                // get geo ids and corresponding tag ids
                var id_pairs = new Dictionary<long, int>();
                var tag_ids = new HashSet<int>();

                for (var index = 0; index <= geo_ids.Count / 1000; ++index)
                {
                    var start = index * 1000;
                    var end = System.Math.Min((index + 1) * 1000, geo_ids.Count);
                    var ids = SchemaTools.ConstructIdList(geo_ids, start, end);

                    if (ids.Length > 0)
                    {
                        var sql = string.Format(SELECT_GEO_TAG_IDS, type.ToString().ToLower(), ids);

                        using (var command = new SqliteCommand(sql, connection))
                        {
                            using (var reader = command.ExecuteReader())
                            {
                                while (reader.Read())
                                {
                                    var id_pair = ReadOneInt64OneInt32(reader);

                                    id_pairs.Add(id_pair.Item1, id_pair.Item2);
                                    tag_ids.Add(id_pair.Item2);
                                }

                                reader.Close();
                            }
                        }
                    }
                }

                // if we actually fetched id pairs
                if (tag_ids.Count > 0)
                {
                    var tags_collections = new Dictionary<int, TagsCollectionBase>();

                    for (var index = 0; index <= tag_ids.Count / 1000; ++index)
                    {
                        var start = index * 1000;
                        var end = System.Math.Min((index + 1) * 1000, tag_ids.Count);
                        var ids = SchemaTools.ConstructIdList(tag_ids.ToList(), start, end);

                        if (ids.Length > 0)
                        {
                            var sql = string.Format(SELECT_TAGS_ID, type.ToString().ToLower(), ids);

                            using (var command = new SqliteCommand(sql, connection))
                            {
                                using (var reader = command.ExecuteReader())
                                {
                                    while (reader.Read())
                                    {
                                        var tag = ReadInt32AndTag(reader);

                                        if (!tags_collections.ContainsKey(tag.Item1))
                                        {
                                            tags_collections[tag.Item1] = new TagsCollection();
                                        }

                                        tags_collections[tag.Item1].Add(tag.Item2);
                                    }
                                }
                            }
                        }
                    }

                    // match up fetched tag collection
                    foreach (var id_pair in id_pairs)
                    {
                        if (id_geos[id_pair.Key].Tags == null)
                        {
                            id_geos[id_pair.Key].Tags = tags_collections[id_pair.Value];
                        }
                        else
                        {
                            id_geos[id_pair.Key].Tags = id_geos[id_pair.Key].Tags.Union(tags_collections[id_pair.Value]);
                        }
                    }
                }
            }
        }

        private void GetTags(SqliteConnection connection, OsmGeo geo)
        {
            var sql = string.Format(SELECT_TAGS, geo.Type.ToString().ToLower(), geo.Id);

            using (var command = new SqliteCommand(sql, connection))
            {
                using (var reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        var tag = ReadTag(reader);

                        if (geo.Tags == null)
                        {
                            geo.Tags = new TagsCollection();
                        }

                        if (!geo.Tags.ContainsKey(tag.Key))
                        {
                            geo.Tags.Add(tag);
                        }
                    }
                }
            }
        }

        private Tag ReadTag(SqliteDataReader reader)
        {
            return new Tag(reader.GetString(0), reader.GetString(1));
        }

        private Tuple<long, Tag> ReadInt64AndTag(SqliteDataReader reader)
        {
            return new Tuple<long, Tag>(reader.GetInt64(0), new Tag(reader.GetString(1), reader.GetString(2)));
        }

        private Tuple<int, Tag> ReadInt32AndTag(SqliteDataReader reader)
        {
            return new Tuple<int, Tag>(reader.GetInt32(0), new Tag(reader.GetString(1), reader.GetString(2)));
        }

        #endregion private tag queries

        #region private node queries

        private IList<Node> GetNodesGivenSQL(IList<long> ids, string sql_command, params object[] args)
        {
            var return_list = new List<Node>();

            if (ids.Count > 0)
            {
                var nodes = new Dictionary<long, Node>();
                for (var index = 0; index <= ids.Count / 1000; index++)
                {
                    var start = index * 1000;
                    var end = System.Math.Min((index + 1) * 1000, ids.Count);
                    var ids_list = SchemaTools.ConstructIdList(ids, start, end);

                    if (ids_list.Length > 0)
                    {
                        string sql;

                        if (args == null)
                        {
                            sql = string.Format(sql_command, ids_list);
                        }
                        else if (args.Length == 0)
                        {
                            sql = string.Format(sql_command, ids_list);
                        }
                        else
                        {
                            var new_args = new List<object>();
                            new_args.Add(ids_list);
                            new_args.AddRange(args);

                            sql = string.Format(sql_command, new_args.ToArray());
                        }

                        using (var command = new SqliteCommand(sql, EnsureConnection()))
                        {
                            using (var reader = command.ExecuteReader())
                            {
                                while (reader.Read())
                                {
                                    var node = ReadNode(reader);

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

                GetTags(EnsureConnection(), nodes);

                return_list = nodes.Values.ToList();
            }
            return return_list;
        }

        private Node ReadNode(SqliteDataReader reader)
        {
            var node = new Node();

            node.Id = reader.GetInt64(0);
            node.Latitude = SQLiteSchemaTools.DBToGeo(reader.GetInt32(1));
            node.Longitude = SQLiteSchemaTools.DBToGeo(reader.GetInt32(2));

            // evaluate name
            if (!reader.IsDBNull(3))
            {
                var name = reader.GetString(3);
                EvaluateName(name, node);
            }

            return node;
        }

        #endregion private node queries

        #region private way queries

        private IList<long> GetWayIdsGivenSQL(string sql_command)
        {
            var way_ids = new List<long>();

            if (!string.IsNullOrEmpty(sql_command))
            {
                using (var command = new SqliteCommand(sql_command, EnsureConnection()))
                {
                    using (var reader = command.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            var way_id = ReadOneInt64(reader);

                            way_ids.Add(way_id);
                        }

                        reader.Close();
                    }
                }
            }

            return way_ids;
        }

        private Way ReadWay(SqliteDataReader reader)
        {
            var way = new Way();

            way.Id = reader.GetInt64(0);

            // evaluate name
            if (!reader.IsDBNull(1))
            {
                var name = reader.GetString(1);
                EvaluateName(name, way);
            }

            return way;
        }

        private void GetWayNodes(SqliteConnection connection, Way way)
        {
            var sql = string.Format(SELECT_WAY_NODE_IDS, way.Id);

            using (var command = new SqliteCommand(sql, connection))
            {
                using (var reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        long node_id = ReadOneInt64(reader);

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

        private void GetWaysNodes(SqliteConnection connection, string way_ids, Dictionary<long, Way> id_ways)
        {
            if (way_ids.Length > 0)
            {
                var sql = string.Format(SELECT_WAYS_IDS_NODES, way_ids);

                using (var command = new SqliteCommand(sql, EnsureConnection()))
                {
                    using (var reader = command.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            var way_id_node_id = ReadTwoInt64s(reader);

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

        #endregion private way queries

        #region private relation queries

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

                var node_ids = SchemaTools.ConstructIdList(nodes.Keys);
                var way_ids = SchemaTools.ConstructIdList(ways.Keys);
                var relation_ids = SchemaTools.ConstructIdList(relations.Keys);

                final_relations.AddRange(GetRelationsFor(node_ids, OsmGeoType.Node));
                final_relations.AddRange(GetRelationsFor(way_ids, OsmGeoType.Way));
                final_relations.AddRange(GetRelationsFor(relation_ids, OsmGeoType.Relation));
            }

            return final_relations;
        }

        private IList<Relation> GetRelationsFor(string ids_string, OsmGeoType member_type)
        {
            if (ids_string.Length > 0)
            {
                var sql = string.Format(SELECT_RELATIONS_FOR_MEMBER, ids_string,
                    SchemaTools.ConvertMemberTypeShort(member_type));
                var relation_ids = new List<long>();

                using (var command = new SqliteCommand(sql, EnsureConnection()))
                {
                    using (var reader = command.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            relation_ids.Add(ReadOneInt64(reader));
                        }
                    }
                }

                return GetRelations(relation_ids);
            }

            return new List<Relation>();
        }

        private Relation ReadRelation(SqliteDataReader reader)
        {
            var relation = new Relation();

            relation.Id = reader.GetInt64(0);

            // evaluate name
            if (!reader.IsDBNull(1))
            {
                var name = reader.GetString(1);
                EvaluateName(name, relation);
            }

            return relation;
        }

        private void GetRelationMembers(SqliteConnection connection, Relation relation)
        {
            var sql = string.Format(SELECT_RELATION_MEMBERS, relation.Id);

            using (var command = new SqliteCommand(sql, connection))
            {
                using (var reader = command.ExecuteReader())
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

        private void GetRelationsMembers(SqliteConnection connection, string relation_ids, Dictionary<long, Relation> id_relations)
        {
            if (relation_ids.Length > 0)
            {
                var sql = string.Format(SELECT_RELATIONS_IDS_MEMBERS, relation_ids);

                using (var command = new SqliteCommand(sql, EnsureConnection()))
                {
                    using (var reader = command.ExecuteReader())
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

        private RelationMember ReadRelationMember(SqliteDataReader reader)
        {
            var relation_member = new RelationMember();

            relation_member.MemberType = SchemaTools.ConvertMemberType(reader.GetInt16(0));
            relation_member.MemberId = reader.GetInt64(1);
            relation_member.MemberRole = reader.GetString(2);

            return relation_member;
        }

        private Tuple<long, RelationMember> ReadRelationIdAndMember(SqliteDataReader reader)
        {
            var relation_member = new RelationMember();

            relation_member.MemberType = SchemaTools.ConvertMemberType(reader.GetInt16(1));
            relation_member.MemberId = reader.GetInt64(2);
            relation_member.MemberRole = reader.GetString(3);

            return new Tuple<long, RelationMember>(reader.GetInt64(0), relation_member);
        }

        #endregion private relation queries

        private static void EvaluateName(string name, OsmGeo geo)
        {
            if (!string.IsNullOrEmpty(name))
            {
                if (geo.Tags == null)
                {
                    geo.Tags = new TagsCollection();
                }

                if (!geo.Tags.ContainsKey("name"))
                {
                    geo.Tags.Add("name", name);
                }
            }
        }
    }
}
