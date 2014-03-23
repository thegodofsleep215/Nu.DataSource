using System;
using System.Collections.Generic;
using System.Data.SQLite;
using System.Linq;
using System.Reflection;
using Nu.DataSource.Attributes;

namespace Nu.DataSource.SQLite
{
    public static class SqliteConnectionExtensions
    {
        /// <summary>
        /// Inserts an item into the database.
        /// </summary>
        /// <typeparam name="T">A class that implements Identity and has a Table attribute.</typeparam>
        /// <param name="conn"></param>
        /// <param name="item">The Item to insert.</param>
        /// <returns>The identity of item after it is inserted.</returns>
        public static long IdentityInsert<T>(this SQLiteConnection conn, T item)
        {
            var ident = (long) conn.InsertAndGetIdent(item);
            var identity = item as Identity;
            if (identity != null) identity.Ident = ident;
            return ident;

        }

        /// <summary>
        /// Replaces the item if it exsits, otherwise it inserts it.
        /// </summary>
        /// <typeparam name="T">A class that implements Identity and has a Table attribute.</typeparam>
        /// <param name="conn"> </param>
        /// <param name="item">The item that is inserted or replaced.</param>
        /// <returns>The identity of the item.</returns>
        public static long IdentityInsertOrReplace<T>(this SQLiteConnection conn, T item) where T : Identity
        {
            var ident = (long) conn.InsertOrReplace(item);
            (item as Identity).Ident = ident;
            return ident;

        }

        /// <summary>
        /// Selects all items of T from the database and saves them to a dictionary.
        /// </summary>
        /// <typeparam name="T">A class that implements Identity and has a Table attribute.</typeparam>
        /// <returns>A dictionary of items keyed by the identity of T</returns>
        public static Dictionary<long, T> IdentityGetAllIntoDictionary<T>(this SQLiteConnection conn) where T : Identity
        {
            return conn.SelectAllIntoDictionary<long, T>(GetKeyColumnFromIdentity<T>());

        }

        /// <summary>
        /// Selects a single item from the database.
        /// </summary>
        /// <typeparam name="T">A class that implements Identity and has a Table attribute.</typeparam>
        /// <param name="conn"></param>
        /// <param name="ident">The identity of the item</param>
        /// <returns>The item from the database, or the default of T if it doesn't exits.</returns>
        public static T IdentitySelect<T>(this SQLiteConnection conn, long ident) where T : Identity
        {
            var paramDict = new Dictionary<string, object>();
            string where = BuildIdentityWhereClause<T>(ident, ref paramDict);
            return conn.SelectObject<T>(ExtensionUtilities.BuildSelectQuery(typeof(T)) + where, paramDict);

        }

        private static string BuildIdentityWhereClause<T>(long ident, ref Dictionary<string, object> paramDict) where T: Identity
        {
            string key = AddIdentityToParamater(ident, ref paramDict);
            return string.Format(" WHERE {0} = {1}", GetKeyColumnFromIdentity<T>(), key);
        }

        private static string AddIdentityToParamater(long ident, ref Dictionary<string, object> paramDict)
        {
            const string baseKey = "@identity";
            if (paramDict == null)
            {
                paramDict = new Dictionary<string, object>{{baseKey, ident}};
                return baseKey;
            }

            int i = 0;
            do
            {
                string key = i == 0 ? baseKey : baseKey + "_" + i;
                if (!paramDict.ContainsKey(key))
                {
                    paramDict[key] = ident;
                    return key;
                }
                i++;
            } while (true);
            
        }

        private static string GetKeyColumnFromIdentity<T>() where T : Identity
        {
            var attribute = (ColumnAttribute)typeof(T).GetProperty("Ident").GetCustomAttribute(typeof(ColumnAttribute));
            string keyColumn = attribute == null ? "ident" : attribute.DbName;
            return keyColumn;
        }

        public static SQLiteCommand CreateCommand(this SQLiteConnection conn, string query, Dictionary<string, object> paramDict)
        {
            SQLiteCommand command = conn.CreateCommand();
            command.CommandText = query;

            foreach (string key in paramDict.Keys)
            {
                SQLiteParameter param = command.CreateParameter();
                param.ParameterName = key;
                param.Value = paramDict[key];
                command.Parameters.Add(param);
            }

            return command;
        }

        public static SQLiteCommand CreateCommand(this SQLiteConnection conn, string query)
        {
            SQLiteCommand command = conn.CreateCommand();
            command.CommandText = query;
            return command;
        }

        public static int ExecuteNonQuery(this SQLiteConnection conn, string query, Dictionary<string, object> paramDict)
        {
            paramDict = paramDict ?? new Dictionary<string, object>();
            SQLiteCommand command = conn.CreateCommand(query, paramDict);
            return command.ExecuteNonQuery();
        }

        public static object InsertAndGetIdent(this SQLiteConnection conn, object obj)
        {
            Dictionary<string, object> paramDict;
            string query = ExtensionUtilities.BuildInsertQuery(obj, out paramDict);
            SQLiteCommand command = CreateCommand(conn, query, paramDict);
            command.ExecuteNonQuery();

            query = "select last_insert_rowid()";
            command = CreateCommand(conn, query);

            return command.ExecuteScalar();
        }

        public static void Insert(this SQLiteConnection conn, object obj)
        {
            Dictionary<string, object> paramDict;
            string query = ExtensionUtilities.BuildInsertQuery(obj, out paramDict);
            SQLiteCommand command = CreateCommand(conn, query, paramDict);
            command.ExecuteNonQuery();            
        }

        public static void UpdateObject(this SQLiteConnection conn, object obj)
        {
            Dictionary<string, object> paramDict;
            string query = ExtensionUtilities.BuildUpdateQuery(obj, out paramDict);
            SQLiteCommand command = CreateCommand(conn, query, paramDict);
            command.ExecuteNonQuery();
        }

        public static object InsertOrReplace(this SQLiteConnection conn, object obj)
        {
            Dictionary<string, object> paramDict;
            string query = ExtensionUtilities.BuildInsertOrReplaceQuery(obj, out paramDict);
            SQLiteCommand command = CreateCommand(conn, query, paramDict);
            command.ExecuteNonQuery();

            query = "select last_insert_rowid()";
            command = CreateCommand(conn, query);

            return command.ExecuteScalar();
        }

        public static void Delete(this SQLiteConnection conn, object obj)
        {
            Dictionary<string, object> paramDict;
            string query = ExtensionUtilities.BuildDeleteQuery(obj, out paramDict);
            SQLiteCommand command = CreateCommand(conn, query, paramDict);
            command.ExecuteNonQuery();
        }

        public static void Create(this SQLiteConnection conn, Type t)
        {
            string query = ExtensionUtilities.BuildCreate(t);
            SQLiteCommand command = CreateCommand(conn, query);
            command.ExecuteNonQuery();
        }

        public static T SelectObject<T>(this SQLiteConnection conn, string query)
        {
            return SelectObject<T>(conn, query, null);
        }

        public static T SelectObject<T>(this SQLiteConnection conn, string query, Dictionary<string, object> paramDict)
        {
            paramDict = paramDict ?? new Dictionary<string, object>();
            SQLiteCommand command = conn.CreateCommand(query, paramDict);
            SQLiteDataReader reader = command.ExecuteReader();
            if (!reader.IsClosed && reader.HasRows)
            {
                reader.Read();
                var row = new Dictionary<string, object>();
                for (int i = 0; i < reader.FieldCount; i++)
                    row.Add(reader.GetName(i), reader[i]);
                return ExtensionUtilities.BuildObject<T>(row);

            }
            return default(T);
        }    

        public static List<T> SelectIntoList<T>(this SQLiteConnection conn)
        {
            var item = SelectIntoList<T>(conn, ExtensionUtilities.BuildSelectQuery(typeof(T)));

            SelectTableReferences(conn, item);

            return item;
        }

        public static List<T> SelectIntoList<T>(this SQLiteConnection conn, string query, Dictionary<string, object> paramDict = null)
        {
            paramDict = paramDict ?? new Dictionary<string, object>();
            var command = conn.CreateCommand(query, paramDict);
            var result = new List<T>();
            var reader = command.ExecuteReader();
            if (!reader.IsClosed && reader.HasRows)
            {
                while (reader.Read())
                {
                    var row = new Dictionary<string, object>();
                    for (int i = 0; i < reader.FieldCount; i++)
                        row.Add(reader.GetName(i), reader[i]);
                    result.Add(ExtensionUtilities.BuildObject<T>(row));
                }
            }
            return result;
        }

        /// <summary>
        /// Selects all items of T from the database using the given where clause and saves them to a list.
        /// </summary>
        /// <typeparam name="T">A class that implements Identity and has a Table attribute.</typeparam>
        /// <returns>A list of items of T</returns>
        public static List<T> SelectItemIntoList<T>(this SQLiteConnection conn, string where, Dictionary<string, object> paramDict = null)
        {
            var query = ExtensionUtilities.BuildSelectQuery(typeof (T)) + " " + where;
            return SelectIntoList<T>(conn, query, paramDict);
        }

        private static void SelectTableReferences<T>(SQLiteConnection conn, List<T> item)
        {
            var props = typeof (T).GetProperties();
            foreach (var p in props)
            {
                var att = (TableReferenceAttribute)p.GetCustomAttribute(typeof(TableReferenceAttribute), true);
                if (att == null)
                {
                    continue;
                }

                string pKey = "@" + att.ForeignKey;
                string where = string.Format("WHERE {0}={1}", att.ForeignKey, pKey);
                if (p.PropertyType.Name.Contains("List"))
                {
                    foreach (var i in item)
                    {
                        MethodInfo mInfo = typeof(SqliteConnectionExtensions).GetMethod("SelectItemIntoList", new[] { typeof(SQLiteConnection), typeof(string), typeof(Dictionary<string, object>) });

                        var paramDict = new Dictionary<string, object>
                            {
                                {pKey, GetForeignKeyColumnValue(i, att)}
                            };
                        MethodInfo mGen = mInfo.MakeGenericMethod(new[] {att.Table});
                        var args = new object[] {conn, where, paramDict};
                        var result = mGen.Invoke(null, args);
                        p.SetValue(i, result);
                    }
                }
            }
        }

        private static object GetForeignKeyColumnValue<T>(T i, TableReferenceAttribute tr)
        {
            foreach (var pInfo in typeof (T).GetProperties())
            {
                var column = pInfo.GetCustomAttribute<ColumnAttribute>();
                if (column == null)
                {
                    continue;
                }
                if (column.DbName == tr.Column)
                {
                    return pInfo.GetValue(i);
                }
            }
            throw new Exception(string.Format("Type, {0}, did not have a property with the Column attribute whos DbName matched {1}", typeof (T).FullName, tr.Column));
        }

        public static Dictionary<TKey, TValue> SelectAllIntoDictionary<TKey, TValue>(this SQLiteConnection conn, string keyColumn = "")
        {
            return SelectIntoDictionary<TKey, TValue>(conn, ExtensionUtilities.BuildSelectQuery(typeof(TValue)), keyColumn);
        }

        public static Dictionary<TKey, TValue> SelectIntoDictionary<TKey, TValue>(this SQLiteConnection conn, string query, string keyColumn = "", Dictionary<string, object> paramDict = null)
        {
            string primaryKey = keyColumn.Length > 0 ? keyColumn : ExtensionUtilities.GetPrimaryKey<TKey, TValue>();
            var result = new Dictionary<TKey, TValue>();
            paramDict = paramDict ?? new Dictionary<string, object>();
            SQLiteCommand command = conn.CreateCommand(query, paramDict);
            SQLiteDataReader reader = command.ExecuteReader();
            if(!reader.IsClosed && reader.HasRows)
            {
                while (reader.Read())
                {
                    var row = new Dictionary<string, object>();
                    for (int i = 0; i < reader.FieldCount; i++)
                        row.Add(reader.GetName(i), reader[i]);
                    var item = ExtensionUtilities.BuildObject<TKey, TValue>(row, primaryKey);
                    result.Add(item.Key, item.Value);
                }
            }
            SelectTableReferences(conn, result.Values.ToList());
            return result;
        }

    }
}
