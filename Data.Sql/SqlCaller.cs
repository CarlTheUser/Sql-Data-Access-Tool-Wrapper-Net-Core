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
        protected readonly ISqlProvider _provider;

        public ISqlProvider Provider => _provider;

        public SqlCaller(ISqlProvider sqlProvider)
        {
            _provider = sqlProvider;
        }

        public DataTable Query(DbCommand command)
        {
            using DbConnection connection = _provider.CreateConnection();
            command.Connection = connection;
            try
            {
                connection.Open();

                using DbDataReader dr = command.ExecuteReader();

                DataTable dt = new();

                dt.Load(dr);

                return dt;
            }
            finally
            {
                command.Connection = null;
                connection.Close();
            }
        }

        public DataTable Query(string queryString)
        {
            return Query(_provider.CreateCommand(queryString));
        }

        public async Task<DataTable> QueryAsync(DbCommand command, CancellationToken token)
        {
            using DbConnection connection = _provider.CreateConnection();
            command.Connection = connection;
            try
            {
                await connection.OpenAsync(token);
                using DbDataReader dr = await command.ExecuteReaderAsync(token);
                DataTable table = new();
                table.Load(dr);
                return table;
            }
            finally
            {
                await connection.CloseAsync();
            }
        }

        public async Task<DataTable> QueryAsync(string queryString, CancellationToken token)
        {
            return await QueryAsync(_provider.CreateCommand(queryString), token);
        }

        public DataTable? GetSchema(string queryString)
        {
            using DbConnection connection = _provider.CreateConnection();
            using DbCommand command = connection.CreateCommand();
            command.CommandText = queryString;
            try
            {
                connection.Open();
                using DbDataReader dr = command.ExecuteReader();
                return dr.GetSchemaTable();
            }
            finally
            {
                connection.Close();
            }
        }

        public async Task<DataTable?> GetSchemaAsync(string queryString, CancellationToken token)
        {
            using (DbConnection connection = _provider.CreateConnection())
            {
                using DbCommand command = connection.CreateCommand();
                command.CommandText = queryString;

                try
                {
                    await connection.OpenAsync(token);
                    using DbDataReader dr = await command.ExecuteReaderAsync(token);
                    return await dr.GetSchemaTableAsync(token);
                }
                finally
                {
                    await connection.CloseAsync();
                }
            }
        }

        public int ExecuteNonQuery(DbCommand command)
        {
            int affectedRows = 0;

            using (DbConnection connection = _provider.CreateConnection())
            {
                command.Connection = connection;
                try
                {
                    connection.Open();

                    affectedRows = command.ExecuteNonQuery();
                }
                finally
                {
                    command.Connection = null;
                    connection.Close();
                }
            }
            return affectedRows;
        }

        public int ExecuteNonQuery(string commandString)
        {
            return ExecuteNonQuery(_provider.CreateCommand(commandString));
        }

        public async Task<int> ExecuteNonQueryAsync(DbCommand command, CancellationToken token)
        {
            using DbConnection connection = _provider.CreateConnection();
            command.Connection = connection;
            try
            {
                await connection.OpenAsync(token);

                return await command.ExecuteNonQueryAsync(token);
            }
            finally
            {
                command.Connection = null;
                await connection.CloseAsync();
            }
        }

        public async Task<int> ExecuteNonQueryAsync(string commandString, CancellationToken token)
        {
            return await ExecuteNonQueryAsync(_provider.CreateCommand(commandString), token);
        }

        public object? ExecuteScalar(DbCommand command)
        {
            using DbConnection connection = _provider.CreateConnection();
            command.Connection = connection;
            try
            {
                connection.Open();

                return command.ExecuteScalar();
            }
            finally
            {
                command.Connection = null;
                connection.Close();
            }
        }

        public object? ExecuteScalar(string queryString)
        {
            return ExecuteScalar(_provider.CreateCommand(queryString));
        }

        public async Task<object?> ExecuteScalarAsync(DbCommand command, CancellationToken token)
        {
            using DbConnection connection = _provider.CreateConnection();
            command.Connection = connection;
            try
            {
                await connection.OpenAsync(token);

                return await command.ExecuteScalarAsync(token);
            }
            finally
            {
                command.Connection = null;
                await connection.CloseAsync();
            }
        }

        public async Task<object?> ExecuteScalarAsync(string queryString, CancellationToken token)
        {
            return await ExecuteScalarAsync(_provider.CreateCommand(queryString), token);
        }

        public void Transact(IsolationLevel isolationLevel, Queue<Action<DbCommand>> commandActions, Action<string> onCommandFailed)
        {
            if (commandActions.FirstOrDefault() == null) return;

            using (DbConnection connection = _provider.CreateConnection())
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

        public async Task TransactAsync(IsolationLevel isolationLevel, Queue<Action<DbCommand>> commandActions, Action<string> onCommandFailed, CancellationToken token)
        {
            if (!commandActions.Any()) return;

            using DbConnection connection = _provider.CreateConnection();

            DbCommand command = connection.CreateCommand();

            DbTransaction? transaction = default;

            try
            {
                await connection.OpenAsync(token);
                transaction = await connection.BeginTransactionAsync(isolationLevel, token);
                command.Transaction = transaction;

                foreach (Action<DbCommand> commandAction in commandActions)
                {
                    commandAction.Invoke(command);
                    await command.ExecuteNonQueryAsync(token);
                    command.Parameters.Clear();
                }

                await transaction.CommitAsync(token);
            }
            catch
            {
                if(transaction != null)
                {
                    await transaction.RollbackAsync(token);
                }

                if (onCommandFailed != null) onCommandFailed.Invoke(command.CommandText);

                throw;
            }
            finally
            {
                await connection.CloseAsync();
            }
        }

        public void OperateCollection<T>(IEnumerable<T> collection, Action<DbCommand> commandInitializer, Action<DbCommand, T> bindingAction, IsolationLevel isolationLevel, Action<T> onItemFail)
        {
            if (collection.FirstOrDefault() == null) return;

            DbConnection connection = _provider.CreateConnection();
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

        public async Task OperateCollectionAsync<T>(IEnumerable<T> collection, Action<DbCommand> commandInitializer, Action<DbCommand, T> bindingAction, IsolationLevel isolationLevel, Action<T> onItemFail, CancellationToken token)
        {
            if (!collection.Any()) return;

            DbConnection connection = _provider.CreateConnection();
            DbTransaction? transaction = default;
            DbCommand command = connection.CreateCommand();

            T[] copy = collection.ToArray();

            int count = copy.Length;

            T current = default!;

            try
            {
                await connection.OpenAsync(token);

                transaction = await connection.BeginTransactionAsync(isolationLevel, token);

                command.Transaction = transaction;

                commandInitializer.Invoke(command);

                for (int i = 0; i != count; ++i)
                {
                    current = copy[i];
                    bindingAction.Invoke(command, current);
                    await command.ExecuteNonQueryAsync(token);
                    command.Parameters.Clear();
                }

                await transaction.CommitAsync(token);
            }
            catch
            {
                if(transaction != null)
                {
                    await transaction.RollbackAsync(token);
                }

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
            return new SqlTransaction(_provider.CreateOpenedConnection(), isolationLevel);
        }

        public IEnumerable<T> Get<T>(Func<IDataReader, List<T>> mappingMethod, string query)
        {
            return Get(mappingMethod, _provider.CreateCommand(query));
        }

        public IEnumerable<T> Get<T>(Func<IDataReader, List<T>> mappingMethod, DbCommand command)
        {
            if (mappingMethod == null) throw new ArgumentNullException(nameof(mappingMethod));

            List<T> temp;

            using (DbConnection connection = _provider.CreateConnection())
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
            List<T> temp = new();

            using (command)
            {
                using DbConnection connection = command.Connection ??= _provider.CreateConnection();
                try
                {
                    connection.Open();

                    IDataReader reader = command.ExecuteReader();

                    while (reader.Read()) temp.Add(dataMapper.CreateMappedInstance(reader));

                }
                finally
                {
                    connection.Close();
                    command.Connection = null;
                }
            }

            return temp;
        }

        public IEnumerable<T> Get<T>(IDataMapper<T> dataMapper, string query) where T : class, new()
        {
            return Get(dataMapper, _provider.CreateCommand(query));
        }

        public IEnumerable<T> Get<T>(DbCommand command) where T : class, new()
        {
            return Get(new ReflectionDataMapper<T>(), command);
        }

        public IEnumerable<T> Get<T>(string query) where T : class, new()
        {
            return Get<T>(_provider.CreateCommand(query));
        }

        public async Task<IEnumerable<T>> GetAsync<T>(IDataMapper<T> dataMapper, DbCommand command, CancellationToken token) where T : class, new()
        {
            List<T> temp = new();

            using (DbConnection connection = command.Connection ??= _provider.CreateConnection())
            {
                try
                {
                    await connection.OpenAsync(token);

                    using DbDataReader reader = await command.ExecuteReaderAsync(token);
                    while (await reader.ReadAsync(token)) temp.Add(dataMapper.CreateMappedInstance(reader));
                }
                finally 
                {
                    connection.Close();
                    command.Connection = null;
                }
            }

            return temp;
        }

        public async Task<IEnumerable<T>> GetAsync<T>(IDataMapper<T> dataMapper, string query, CancellationToken token) where T : class, new()
        {
            return await GetAsync(dataMapper, _provider.CreateCommand(query), token);
        }

        public async Task<IEnumerable<T>> GetAsync<T>(IDataMapper<T> dataMapper, DbCommand command) where T : class, new()
        {
            return await GetAsync(dataMapper, command, CancellationToken.None);
        }

        public async Task<IEnumerable<T>> GetAsync<T>(IDataMapper<T> dataMapper, string query) where T : class, new()
        {
            return await GetAsync(dataMapper, _provider.CreateCommand(query));
        }

        public async Task<IEnumerable<T>> GetAsync<T>(DbCommand command) where T : class, new()
        {
            return await GetAsync(new ReflectionDataMapper<T>(), command);
        }

        public async Task<IEnumerable<T>> GetAsync<T>(string query) where T : class, new()
        {
            return await GetAsync<T>(_provider.CreateCommand(query));
        }

        public IEnumerable<dynamic> GetDynamic(DbCommand command)
        {
            throw new NotImplementedException();
        }
        public IEnumerable<dynamic> GetDynamic(string commandString)
        {
            throw new NotImplementedException();
        }

        public Task<IEnumerable<dynamic>> GetDynamicAsync(DbCommand command, CancellationToken token)
        {
            throw new NotImplementedException();
        }

        public void Iterate<T>(IDataMapper<T> dataMapper, Action<T> iteratorAction, DbCommand command) where T : class, new()
        {
            using DbConnection connection = command.Connection ??= _provider.CreateConnection();
            try
            {
                connection.Open();

                using DbDataReader reader = command.ExecuteReader();
                while (reader.Read()) iteratorAction.Invoke(dataMapper.CreateMappedInstance(reader));
            }
            finally
            {
                connection.Close();
                command.Connection = null;
            }
        }

        public void Iterate<T>(IDataMapper<T> dataMapper, Action<T> iteratorAction, string query) where T : class, new()
        {
            Iterate(dataMapper, iteratorAction, _provider.CreateCommand(query));
        }

        public async Task IterateAsync<T>(IDataMapper<T> dataMapper, Action<T> iteratorAction, DbCommand command, CancellationToken token) where T : class, new()
        {
            using DbConnection connection = command.Connection ??= _provider.CreateConnection();
            try
            {
                await connection.OpenAsync(token);

                using DbDataReader reader = await command.ExecuteReaderAsync(token);

                while (await reader.ReadAsync(token))
                {
                    iteratorAction.Invoke(dataMapper.CreateMappedInstance(reader));
                }
            }
            finally
            { 
                connection.Close();
                command.Connection = null;
            }
        }

        public async Task IterateAsync<T>(IDataMapper<T> dataMapper, Action<T> iteratorAction, string query, CancellationToken token) where T : class, new()
        {
            await IterateAsync(dataMapper, iteratorAction, _provider.CreateCommand(query), token);
        }
    }
}
