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
using OsmSharp.Osm;
using OsmSharp.Osm.Streams;
using System;
using System.Collections.Generic;
using Mono.Data.Sqlite;

namespace OsmSharp.Data.SQLite.Osm.Streams
{
    /// <summary>
    /// An SQLite data processor source.
    /// </summary>
    public class SQLiteOsmStreamSource : OsmStreamSource
    {
        #region fields

        /// <summary>
        /// Holds the connection.
        /// </summary>
        private SqliteConnection _connection;

        /// <summary>
        /// Holds the connection string.
        /// </summary>
        private readonly string _connection_string;

        /// <summary>
        /// Flag that indicates if the schema needs to be created if not present.
        /// </summary>
        private readonly bool _create_and_detect_schema;

        /// <summary>
        /// The unique id for this datasource.
        /// </summary>
        private readonly Guid _id;

        /// <summary>
        /// Holds the current type.
        /// </summary>
        private OsmGeoType _current_type;

        /// <summary>
        /// Holds the current object.
        /// </summary>
        private OsmGeo _current;

        #endregion fields

        #region constructors

        /// <summary>
        /// Creates a new SQLite data processor source.
        /// </summary>
        /// <param name="connection_string">The connection string for the SQLite db</param>
        /// <param name="create_schema">Do the db schema and tables need to be created?</param>
        public SQLiteOsmStreamSource(string connection_string, bool create_schema = false)
        {
            _connection_string = connection_string;
            _id = Guid.NewGuid();
            _create_and_detect_schema = create_schema;

            _connection = EnsureConnection();
        }

        /// <summary>
        /// Creates a new SQLite data processor source.
        /// </summary>
        /// <param name="in_memory">Is the DB in memory? (default is false)</param>
        /// <param name="path">The path to the DB, or its descriptor in memory (if any)</param>
        /// <param name="password">The DB password (if any)</param>
        /// <param name="create_schema">Do the db tables need to be created?</param>
        public SQLiteOsmStreamSource(bool in_memory = false, string path = null,
                                     string password = null, bool create_schema = false)
        {
            _connection_string = SQLiteSchemaTools.BuildConnectionString(in_memory, path, password);
            _id = Guid.NewGuid();
            _create_and_detect_schema = create_schema;

            _connection = EnsureConnection();
        }

        /// <summary>
        /// Creates a new SQLite data processor source.
        /// </summary>
        /// <param name="connection">The SQLite connection</param>
        /// <param name="create_schema">Do the db schema and tables need to be created?</param>
        public SQLiteOsmStreamSource(SqliteConnection connection, bool create_schema = false)
        {
            _connection = connection;
            _id = Guid.NewGuid();

            _connection_string = connection.ConnectionString;
            _create_and_detect_schema = create_schema;

            _connection = EnsureConnection();
        }

        #endregion constructors

        private SqliteConnection EnsureConnection()
        {
            if (_connection == null)
            {
                _connection = new SqliteConnection(_connection_string);
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

        /// <summary>
        /// Initializes this source.
        /// </summary>
        public override void Initialize()
        {
            _current = null;
            _current_type = OsmGeoType.Node;

            _node_reader = null;
        }

        /// <summary>
        /// Move to the next item in the stream.
        /// </summary>
        /// <param name="ignoreNodes">Makes this source skip all nodes.</param>
        /// <param name="ignoreWays">Makes this source skip all ways.</param>
        /// <param name="ignoreRelations">Makes this source skip all relations.</param>
        /// <returns></returns>
        public override bool MoveNext(bool ignoreNodes, bool ignoreWays, bool ignoreRelations)
        {
            bool next = false;
            switch (_current_type)
            {
                case OsmGeoType.Node:
                    while (this.DoMoveNextNode())
                    {
                        if (!ignoreNodes)
                        {
                            return true;
                        }
                    }
                    return this.MoveNext(ignoreNodes, ignoreWays, ignoreRelations);
                case OsmGeoType.Way:
                    if (this.DoMoveNextWay())
                    {
                        if (!ignoreWays)
                        {
                            return true;
                        }
                    }
                    return this.MoveNext(ignoreNodes, ignoreWays, ignoreRelations);
                case OsmGeoType.Relation:
                    if (ignoreRelations)
                    {
                        return false;
                    }
                    return this.DoMoveNextRelation();
            }
            return next;
        }

        /// <summary>
        /// Returns true if this source is sorted.
        /// </summary>
        public override bool IsSorted
        {
            get
            {
                return true;
            }
        }

        #region MoveNext fuctions

        private SqliteDataReader _node_reader;
        private SqliteDataReader _way_reader;
        private SqliteDataReader _way_tag_reader;
        private SqliteDataReader _way_node_reader;
        private SqliteDataReader _relation_reader;
        private SqliteDataReader _relation_tag_reader;
        private SqliteDataReader _relation_member_reader;

        private bool DoMoveNextRelation()
        {
            if (_relation_reader == null)
            {
                var relationCommand = new SqliteCommand("select * from relation order by id", _connection);
                _relation_reader = relationCommand.ExecuteReader();
                if (!_relation_reader.Read())
                {
                    _relation_reader.Close();
                }
                var relationTagCommand = new SqliteCommand("select * from relation_tags order by relation_id", _connection);
                _relation_tag_reader = relationTagCommand.ExecuteReader();
                if (!_relation_tag_reader.IsClosed && !_relation_tag_reader.Read())
                {
                    _relation_tag_reader.Close();
                }
                var relationNodeCommand = new SqliteCommand("select * from relation_members order by relation_id,sequence_id", _connection);
                _relation_member_reader = relationNodeCommand.ExecuteReader();
                if (!_relation_member_reader.IsClosed && !_relation_member_reader.Read())
                {
                    _relation_member_reader.Close();
                }
            }

            // read next relation.
            if (!_relation_reader.IsClosed)
            {
                // load/parse data.
                long id = _relation_reader.GetInt64(0);
                long changesetId = _relation_reader.GetInt64(1);
                bool visible = _relation_reader.GetInt64(2) == 1;
                DateTime timestamp = _relation_reader.IsDBNull(3) ? DateTime.MinValue : _relation_reader.GetDateTime(3);
                long version = _relation_reader.GetInt64(4);
                string user = _relation_reader.GetString(5);
                long uid = _relation_reader.GetInt64(6);
                var relation = new Relation
                {
                    Id = id,
                    ChangeSetId = changesetId,
                    TimeStamp = timestamp,
                    UserId = null,
                    UserName = null,
                    Version = (ulong)version,
                    Visible = visible
                };
                relation.UserName = user;
                relation.UserId = uid;

                if (!_relation_tag_reader.IsClosed)
                {
                    long returnedId = _relation_tag_reader.GetInt64(0);
                    while (returnedId == relation.Id.Value)
                    {
                        if (relation.Tags == null)
                        {
                            relation.Tags = new TagsCollection();
                        }
                        string key = _relation_tag_reader.GetString(1);
                        string value = _relation_tag_reader.GetString(2);

                        relation.Tags.Add(key, value);

                        if (!_relation_tag_reader.Read())
                        {
                            _relation_tag_reader.Close();
                            returnedId = -1;
                        }
                        else
                        {
                            returnedId = _relation_tag_reader.GetInt64(0);
                        }
                    }
                }
                if (!_relation_member_reader.IsClosed)
                {
                    long returnedId = _relation_member_reader.GetInt64(0);
                    while (returnedId == relation.Id.Value)
                    {
                        if (relation.Members == null)
                        {
                            relation.Members = new List<RelationMember>();
                        }
                        string memberType = _relation_member_reader.GetString(1);
                        long memberId = _relation_member_reader.GetInt64(2);
                        object memberRole = _relation_member_reader.GetValue(3);

                        var member = new RelationMember();
                        member.MemberId = memberId;
                        if (memberRole != DBNull.Value)
                        {
                            member.MemberRole = memberRole as string;
                        }
                        switch (memberType)
                        {
                            case "Node":
                                member.MemberType = OsmGeoType.Node;
                                break;
                            case "Way":
                                member.MemberType = OsmGeoType.Way;
                                break;
                            case "Relation":
                                member.MemberType = OsmGeoType.Relation;
                                break;
                        }

                        relation.Members.Add(member);

                        if (!_relation_member_reader.Read())
                        {
                            _relation_member_reader.Close();
                            returnedId = -1;
                        }
                        else
                        {
                            returnedId = _relation_member_reader.GetInt64(0);
                        }
                    }
                }

                // set the current variable!
                _current = relation;

                // advance the reader(s).
                if (!_relation_reader.Read())
                {
                    _relation_reader.Close();
                }
                if (!_relation_tag_reader.IsClosed && !_relation_tag_reader.Read())
                {
                    _relation_tag_reader.Close();
                }
                if (!_relation_member_reader.IsClosed && !_relation_member_reader.Read())
                {
                    _relation_member_reader.Close();
                }
                return true;
            }
            else
            {
                _relation_reader.Close();
                _relation_reader.Dispose();
                _relation_reader = null;

                _relation_tag_reader.Close();
                _relation_tag_reader.Dispose();
                _relation_tag_reader = null;

                _current_type = OsmGeoType.Relation;

                return false;
            }
        }

        private bool DoMoveNextNode()
        {
            if (_node_reader == null)
            {
                SqliteCommand node_command = new SqliteCommand("select * from node left join node_tags on node_tags.node_id = node.id order by node.id");
                node_command.Connection = _connection;
                _node_reader = node_command.ExecuteReader();
                if (!_node_reader.Read())
                    _node_reader.Close();
            }

            // read next node.
            if (!_node_reader.IsClosed)
            {
                // load/parse data.
                Node node = new Node();
                node.Id = _node_reader.GetInt64(0);
                node.Latitude = _node_reader.GetInt64(1) / 10000000.0;
                node.Longitude = _node_reader.GetInt64(2) / 10000000.0;
                node.ChangeSetId = _node_reader.GetInt64(3);
                node.TimeStamp = _node_reader.GetDateTime(5);
                node.Version = (ulong)_node_reader.GetInt64(7);
                node.Visible = _node_reader.GetInt64(4) == 1;
                //node.UserName = _node_reader.GetString(8);
                //node.UserId = _node_reader.IsDBNull(9) ? -1 : _node_reader.GetInt64(9);

                //Has tags?
                if (!_node_reader.IsDBNull(10))
                {
                    //if (node.Tags == null)
                    //node.Tags = new Dictionary<string, string>();

                    long currentnode = node.Id.Value;
                    while (currentnode == node.Id.Value)
                    {
                        //string key = _node_reader.GetString(11);
                        //string value = _node_reader.GetString(12);
                        //node.Tags.Add(key, value);
                        if (!_node_reader.Read())
                        {
                            _node_reader.Close();
                            break;
                        }
                        currentnode = _node_reader.GetInt64(0);
                    }
                }
                else if (!_node_reader.Read())
                    _node_reader.Close();
                // set the current variable!
                _current = node;
                return true;
            }
            _node_reader.Close();
            _node_reader.Dispose();
            _node_reader = null;
            _current_type = OsmGeoType.Way;
            return false;
        }

        private bool DoMoveNextWay()
        {
            if (_way_reader == null)
            {
                SqliteCommand way_command = new SqliteCommand("select * from way where id > 26478817 order by id");
                way_command.Connection = _connection;
                _way_reader = way_command.ExecuteReader();
                if (!_way_reader.Read())
                {
                    _way_reader.Close();
                }
                SqliteCommand way_tag_command = new SqliteCommand("select * from way_tags where way_id > 26478817 order by way_id");
                way_tag_command.Connection = _connection;
                _way_tag_reader = way_tag_command.ExecuteReader();
                if (!_way_tag_reader.IsClosed && !_way_tag_reader.Read())
                {
                    _way_tag_reader.Close();
                }
                SqliteCommand way_node_command = new SqliteCommand("select * from way_nodes where way_id > 26478817 order by way_id,sequence_id");
                way_node_command.Connection = _connection;
                _way_node_reader = way_node_command.ExecuteReader();
                if (!_way_node_reader.IsClosed && !_way_node_reader.Read())
                {
                    _way_node_reader.Close();
                }
            }

            // read next way.
            if (!_way_reader.IsClosed)
            {

                Way way = new Way();
                way.Id = _way_reader.GetInt64(0);
                way.ChangeSetId = _way_reader.GetInt64(1);
                way.TimeStamp = _way_reader.IsDBNull(3) ? DateTime.MinValue : _way_reader.GetDateTime(3);
                //way.UserId = _way_reader.GetInt64(6);
                //way.UserName = _way_reader.GetString(5);
                way.Version = (ulong)_way_reader.GetInt64(4);
                way.Visible = _way_reader.GetInt64(2) == 1;

                if (!_way_tag_reader.IsClosed)
                {
                    long returned_id = _way_tag_reader.GetInt64(_way_tag_reader.GetOrdinal("way_id"));
                    while (returned_id == way.Id.Value)
                    {
                        if (way.Tags == null)
                        {
                            way.Tags = new TagsCollection();
                        }
                        string key = _way_tag_reader.GetString(1);
                        string value = _way_tag_reader.GetString(2);

                        way.Tags.Add(key, value);

                        if (!_way_tag_reader.Read())
                        {
                            _way_tag_reader.Close();
                            returned_id = -1;
                        }
                        else
                        {
                            returned_id = _way_tag_reader.GetInt64(0);
                        }
                    }
                }
                if (!_way_node_reader.IsClosed)
                {
                    long returned_id = _way_node_reader.GetInt64(_way_node_reader.GetOrdinal("way_id"));
                    while (returned_id == way.Id.Value)
                    {
                        if (way.Nodes == null)
                        {
                            way.Nodes = new List<long>();
                        }
                        long node_id = _way_node_reader.GetInt64(1);

                        way.Nodes.Add(node_id);

                        if (!_way_node_reader.Read())
                        {
                            _way_node_reader.Close();
                            returned_id = -1;
                        }
                        else
                        {
                            returned_id = _way_node_reader.GetInt64(0);
                        }
                    }
                }

                // set the current variable!
                _current = way;

                // advance the reader(s).
                if (!_way_reader.Read())
                {
                    _way_reader.Close();
                }
                if (!_way_tag_reader.IsClosed && !_way_tag_reader.Read())
                {
                    _way_tag_reader.Close();
                }
                if (!_way_node_reader.IsClosed && !_way_node_reader.Read())
                {
                    _way_node_reader.Close();
                }
                return true;
            }
            else
            {
                _way_reader.Close();
                _way_reader.Dispose();
                _way_reader = null;

                _way_tag_reader.Close();
                _way_tag_reader.Dispose();
                _way_tag_reader = null;

                _current_type = OsmGeoType.Relation;

                return false;
            }
        }

        #endregion

        /// <summary>
        /// Returns the current object.
        /// </summary>
        /// <returns></returns>
        public override OsmGeo Current()
        {
            return _current;
        }

        /// <summary>
        /// Resets this source.
        /// </summary>
        public override void Reset()
        {
            _current = null;
            _current_type = OsmGeoType.Node;

            if (_node_reader != null)
            {
                _node_reader.Close();
                _node_reader.Dispose();
                _node_reader = null;
            }
        }

        /// <summary>
        /// Returns a value that indicates if this source can be reset or not.
        /// </summary>
        public override bool CanReset
        {
            get { return true; }
        }
    }
}
