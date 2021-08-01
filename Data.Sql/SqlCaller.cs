using Data.Sql.Mapping;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Data.Sql
{
    public class SqlCaller : ISqlCaller
    {
        protected readonly ISqlProvider _sqlProvider;

        public ISqlProvider Provider => _sqlProvider;

        public SqlCaller(ISqlProvider sqlProvider)
        {
            _sqlProvider = sqlProvider;
        }

        public DataTable Query(DbCommand command)
        {
            DataTable dt = default;

            using (DbConnection connection = _sqlProvider.CreateConnection())
            {
                command.Connection = connection;
                try
                {
                    connection.Open();
                    using (DbDataReader dr = command.ExecuteReader())
                    {
                        dt = new DataTable();

                        dt.Load(dr);
                    }
                }
                finally
                {
                    connection.Close();
                }
            }
            return dt;
        }

        public DataTable Query(string queryString)
        {
            return Query(_sqlProvider.CreateCommand(queryString));
        }

        public DataTable GetSchema(string queryString)
        {
            DataTable dt = null;

            using (DbConnection connection = _sqlProvider.CreateConnection())
            {
                using (DbCommand command = connection.CreateCommand())
                {
                    command.CommandText = queryString;

                    try
                    {
                        connection.Open();
                        using (DbDataReader dr = command.ExecuteReader())
                        {
                            dt = dr.GetSchemaTable();
                        }
                    }
                    finally
                    {
                        connection.Close();
                    }
                }
            }

            return dt;
        }

        public int ExecuteNonQuery(DbCommand command)
        {
            int affectedRows = 0;

            using (DbConnection connection = _sqlProvider.CreateConnection())
            {
                command.Connection = connection;
                try
                {
                    connection.Open();

                    affectedRows = command.ExecuteNonQuery();
                }
                finally
                {
                    connection.Close();
                }
            }
            return affectedRows;
        }

        public int ExecuteNonQuery(string commandString)
        {
            return ExecuteNonQuery(_sqlProvider.CreateCommand(commandString));
        }

        public object ExecuteScalar(DbCommand command)
        {
            object returnValue = null;

            using (DbConnection connection = _sqlProvider.CreateConnection())
            {
                command.Connection = connection;

                try
                {
                    connection.Open();

                    returnValue = command.ExecuteScalar();
                }
                finally
                {
                    connection.Close();
                }
            }
            return returnValue;
        }

        public object ExecuteScalar(string queryString)
        {
            return ExecuteScalar(_sqlProvider.CreateCommand(queryString));
        }

        public void Transact(IsolationLevel isolationLevel, Queue<Action<DbCommand>> commandActions, Action<string> onCommandFailed)
        {
            if (commandActions.FirstOrDefault() == null) return;

            using (DbConnection connection = _sqlProvider.CreateConnection())
            {
                DbCommand command = connection.CreateCommand();

                DbTransaction transaction = null;

                try
                {
                    connection.Open();
                    transaction = connection.BeginTransaction(isolationLevel);
                    command.Transaction = transaction;

                    foreach (Action<DbCommand> commandAction in commandActions)
                    {
                        commandAction.Invoke(command);
                        command.ExecuteNonQuery();
                        command.Parameters.Clear();
                    }

                    transaction.Commit();
                }
                catch (Exception e)
                {
                    transaction?.Rollback();

                    if (onCommandFailed != null) onCommandFailed.Invoke(command.CommandText);

                    throw;
                }
                finally
                {
                    connection.Close();
                }
            }
        }

        public void OperateCollection<T>(IEnumerable<T> collection, Action<DbCommand> commandInitializer, Action<DbCommand, T> bindingAction, IsolationLevel isolationLevel, Action<T> onItemFail)
        {
            if (collection.FirstOrDefault() == null) return;

            DbConnection connection = _sqlProvider.CreateConnection();
            DbTransaction transaction = default;
            DbCommand command = connection.CreateCommand();

            T[] copy = collection.ToArray();

            int count = copy.Length;

            T current = default;

            try
            {
                connection.Open();

                transaction = connection.BeginTransaction(isolationLevel);

                command.Transaction = transaction;

                commandInitializer.Invoke(command);

                for (int i = 0; i != count; ++i)
                {
                    current = copy[i];
                    bindingAction.Invoke(command, current);
                    command.ExecuteNonQuery();
                    command.Parameters.Clear();
                }

                transaction.Commit();
            }
            catch (Exception e)
            {
                transaction.Rollback();

                onItemFail.Invoke(current);

                throw;
            }
            finally
            {
                connection.Close();
                command.Dispose();
                connection.Dispose();
            }
        }

        public SqlTransaction CreateScopedTransaction(IsolationLevel isolationLevel)
        {
            return new SqlTransaction(_sqlProvider.CreateOpenedConnection(), isolationLevel);
        }

        public IEnumerable<T> Get<T>(Func<IDataReader, List<T>> mappingMethod, string query)
        {
            return Get(mappingMethod, _sqlProvider.CreateCommand(query));
        }

        public IEnumerable<T> Get<T>(Func<IDataReader, List<T>> mappingMethod, DbCommand command)
        {
            if (mappingMethod == null) throw new ArgumentNullException(nameof(mappingMethod));

            List<T> temp;

            using (DbConnection connection = _sqlProvider.CreateConnection())
            {
                command.Connection = connection;

                try
                {
                    command.Connection.Open();

                    temp = mappingMethod.Invoke(command.ExecuteReader());
                }
                finally
                {
                    command.Connection.Close();
                }
            }

            return temp;
        }

        public IEnumerable<T> Get<T>(IDataMapper<T> dataMapper, DbCommand command) where T : class, new()
        {
            List<T> temp = new List<T>();

            using (command)
            {
                using (DbConnection connection = command.Connection = command.Connection ?? _sqlProvider.CreateConnection())
                {
                    try
                    {
                        connection.Open();

                        IDataReader reader = command.ExecuteReader();

                        while (reader.Read()) temp.Add(dataMapper.CreateMappedInstance(reader));

                    }
                    finally { connection.Close(); }
                }
            }

            return temp;
        }

        public IEnumerable<T> Get<T>(IDataMapper<T> dataMapper, string query) where T : class, new()
        {
            return Get(dataMapper, _sqlProvider.CreateCommand(query));
        }

        public IEnumerable<T> Get<T>(DbCommand command) where T : class, new()
        {
            return Get(new ReflectionDataMapper<T>(), command);
        }

        public IEnumerable<T> Get<T>(string query) where T : class, new()
        {
            return Get<T>(_sqlProvider.CreateCommand(query));
        }

        public async Task<IEnumerable<T>> GetAsync<T>(IDataMapper<T> dataMapper, DbCommand command, CancellationToken token) where T : class, new()
        {
            List<T> temp = new List<T>();

            using (DbConnection connection = command.Connection = command.Connection ?? _sqlProvider.CreateConnection())
            {
                try
                {
                    await connection.OpenAsync(token);

                    using (DbDataReader reader = command.ExecuteReaderAsync(token).Result)
                    {
                        while (await reader.ReadAsync(token)) temp.Add(dataMapper.CreateMappedInstance(reader));
                    }
                }
                finally { connection.Close(); }
            }

            return temp;
        }

        public async Task<IEnumerable<T>> GetAsync<T>(IDataMapper<T> dataMapper, string query, CancellationToken token) where T : class, new()
        {
            return await GetAsync(dataMapper, _sqlProvider.CreateCommand(query), token);
        }

        public async Task<IEnumerable<T>> GetAsync<T>(IDataMapper<T> dataMapper, DbCommand command) where T : class, new()
        {
            CancellationTokenSource source = new CancellationTokenSource();
            return await GetAsync(dataMapper, command, source.Token);
        }

        public async Task<IEnumerable<T>> GetAsync<T>(IDataMapper<T> dataMapper, string query) where T : class, new()
        {
            return await GetAsync(dataMapper, _sqlProvider.CreateCommand(query));
        }

        public async Task<IEnumerable<T>> GetAsync<T>(DbCommand command) where T : class, new()
        {
            return await GetAsync(new ReflectionDataMapper<T>(), command);
        }

        public async Task<IEnumerable<T>> GetAsync<T>(string query) where T : class, new()
        {
            return await GetAsync<T>(_sqlProvider.CreateCommand(query));
        }

        public IEnumerable<dynamic> GetDynamic(string commandString)
        {
            throw new NotImplementedException();
        }

        public IEnumerable<dynamic> GetDynamic(DbCommand command)
        {
            throw new NotImplementedException();
        }

        public Task<IEnumerable<dynamic>> GetDynamicAsync(DbCommand command, CancellationToken token)
        {
            throw new NotImplementedException();
        }

        public void Iterate<T>(IDataMapper<T> dataMapper, Action<T> iteratorAction, DbCommand command) where T : class, new()
        {
            using (DbConnection connection = command.Connection = command.Connection ?? _sqlProvider.CreateConnection())
            {
                try
                {
                    connection.Open();

                    using (DbDataReader reader = command.ExecuteReader())
                    {
                        while (reader.Read()) iteratorAction.Invoke(dataMapper.CreateMappedInstance(reader));
                    }
                }
                finally { connection.Close(); }
            }
        }

        public void Iterate<T>(IDataMapper<T> dataMapper, Action<T> iteratorAction, string query) where T : class, new()
        {
            Iterate(dataMapper, iteratorAction, _sqlProvider.CreateCommand(query));
        }

        public async Task IterateAsync<T>(IDataMapper<T> dataMapper, Action<T> iteratorAction, DbCommand command, CancellationToken token) where T : class, new()
        {
            using (DbConnection connection = command.Connection = command.Connection ?? _sqlProvider.CreateConnection())
            {
                try
                {
                    await connection.OpenAsync(token);

                    using (DbDataReader reader = command.ExecuteReaderAsync(token).Result)
                    {
                        while (await reader.ReadAsync(token)) await Task.Run(() => iteratorAction.Invoke(dataMapper.CreateMappedInstance(reader)), token);
                    }
                }
                finally { connection.Close(); }
            }
        }
    }
}
