using NUnit.Framework;
using OsmSharp.Data.SQLite.Osm;
using System.Data.SQLite;

namespace OsmSharp.Data.Test.Unittests.SQLite
{
    /// <summary>
    /// Contains database tests for SQLite and osm-data.
    /// </summary>
    [TestFixture]
    public class SQLiteInMemoryProviderTests : SQLiteProviderTests
    {
        protected override SQLiteConnection GetConnection()
        {
            if (_connection == null)
            {
                _connection = new SQLiteConnection(@"FullUri=file::memory:?cache=shared;Version=3;");
                _connection.Open();

                SQLiteSchemaTools.Drop(_connection);
            }
            return _connection;
        }
    }
}
