using Data.Sql.Mapping;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Runtime.CompilerServices;
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
            DbConnection connection = _provider.CreateConnection();
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
                connection.Dispose();
            }
        }

        public DataTable Query(string queryString)
        {
            using var command = _provider.CreateCommand(queryString);
            return Query(command);
        }

        public async Task<DataTable> QueryAsync(DbCommand command, CancellationToken cancellationToken = default)
        {
            DbConnection connection = _provider.CreateConnection();

            command.Connection = connection;

            await connection.OpenAsync(cancellationToken);

            DbDataReader reader = await command.ExecuteReaderAsync(cancellationToken);

            try
            {
                var table = new DataTable();

                int columnCount = reader.FieldCount;

                for (int i = 0; i < columnCount; ++i)
                {
                    table.Columns.Add(reader.GetName(i));
                }

                while (await reader.ReadAsync(cancellationToken))
                {
                    var values = new object[columnCount];
                    reader.GetValues(values);
                    table.Rows.Add(values);
                }

                await reader.CloseAsync();

                return table;
            }
            finally
            {
                await reader.DisposeAsync();
                await connection.CloseAsync();
                await connection.DisposeAsync();
            }
        }

        public async Task<DataTable> QueryAsync(string queryString, CancellationToken cancellationToken = default)
        {
            using var command = _provider.CreateCommand(queryString);
            return await QueryAsync(command, cancellationToken);
        }

        public DataTable? GetSchema(string queryString)
        {
            DbConnection connection = _provider.CreateConnection();
            DbCommand command = connection.CreateCommand();
            command.CommandText = queryString;
            try
            {
                connection.Open();
                using DbDataReader dr = command.ExecuteReader();
                return dr.GetSchemaTable();
            }
            finally
            {
                command.Dispose();
                connection.Close();
                connection.Dispose();
            }
        }

        public async Task<DataTable?> GetSchemaAsync(string queryString, CancellationToken cancellationToken = default)
        {
            DbConnection connection = _provider.CreateConnection();
            DbCommand command = connection.CreateCommand();
            command.CommandText = queryString;

            try
            {
                await connection.OpenAsync(cancellationToken);
                using DbDataReader dr = await command.ExecuteReaderAsync(cancellationToken);
                return await dr.GetSchemaTableAsync(cancellationToken);
            }
            finally
            {
                await command.DisposeAsync();
                await connection.CloseAsync();
                await connection.DisposeAsync();
            }
        }

        public int ExecuteNonQuery(DbCommand command)
        {
            DbConnection connection = _provider.CreateConnection();
            command.Connection = connection;
            try
            {
                connection.Open();

                return command.ExecuteNonQuery();
            }
            finally
            {
                command.Connection = null;
                connection.Close();
                connection.Dispose();
            }
        }

        public int ExecuteNonQuery(string commandString)
        {
            using var command = _provider.CreateCommand(commandString);
            return ExecuteNonQuery(command);
        }

        public async Task<int> ExecuteNonQueryAsync(DbCommand command, CancellationToken token)
        {
            DbConnection connection = _provider.CreateConnection();
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
                await connection.DisposeAsync();
            }
        }

        public async Task<int> ExecuteNonQueryAsync(string commandString, CancellationToken cancellationToken = default)
        {
            using var command = _provider.CreateCommand(commandString);
            return await ExecuteNonQueryAsync(command, cancellationToken);
        }

        public object? ExecuteScalar(DbCommand command)
        {
            DbConnection connection = _provider.CreateConnection();
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
                connection.Dispose();
            }
        }

        public object? ExecuteScalar(string queryString)
        {
            using var command = _provider.CreateCommand(queryString);
            return ExecuteScalar(command);
        }

        public async Task<object?> ExecuteScalarAsync(DbCommand command, CancellationToken cancellationToken = default)
        {
            DbConnection connection = _provider.CreateConnection();
            command.Connection = connection;
            try
            {
                await connection.OpenAsync(cancellationToken);

                return await command.ExecuteScalarAsync(cancellationToken);
            }
            finally
            {
                command.Connection = null;
                await connection.CloseAsync();
                await connection.DisposeAsync();
            }
        }

        public async Task<object?> ExecuteScalarAsync(string queryString, CancellationToken cancellationToken = default)
        {
            using var command = _provider.CreateCommand(queryString);
            return await ExecuteScalarAsync(command, cancellationToken);
        }

        public void Transact(IsolationLevel isolationLevel, Queue<Action<DbCommand>> commandActions, Action<string> onCommandFailed)
        {
            if (!commandActions.Any()) return;

            DbConnection connection = _provider.CreateConnection();

            connection.Open();

            DbCommand command = connection.CreateCommand();

            DbTransaction transaction = connection.BeginTransaction(isolationLevel);

            command.Transaction = transaction;

            try
            {
                foreach (Action<DbCommand> commandAction in commandActions)
                {
                    commandAction.Invoke(command);
                    command.ExecuteNonQuery();
                    command.Parameters.Clear();
                }

                transaction.Commit();
            }
            catch
            {
                transaction.Rollback();

                if (onCommandFailed != null) onCommandFailed.Invoke(command.CommandText);

                throw;
            }
            finally
            {
                connection.Close();
                connection.Dispose();
                command.Dispose();
                transaction.Dispose();
            }
        }

        public async Task TransactAsync(IsolationLevel isolationLevel, Queue<Action<DbCommand>> commandActions, Action<string> onCommandFailed, CancellationToken cancellationToken = default)
        {
            if (!commandActions.Any()) return;

            DbConnection connection = _provider.CreateConnection();

            DbCommand command = connection.CreateCommand();

            await connection.OpenAsync(cancellationToken);

            DbTransaction transaction = await connection.BeginTransactionAsync(isolationLevel, cancellationToken);

            command.Transaction = transaction;

            try
            {
                foreach (Action<DbCommand> commandAction in commandActions)
                {
                    commandAction.Invoke(command);
                    await command.ExecuteNonQueryAsync(cancellationToken);
                    command.Parameters.Clear();
                }

                await transaction.CommitAsync(cancellationToken);
            }
            catch
            {
                await transaction.RollbackAsync(cancellationToken);

                if (onCommandFailed != null) onCommandFailed.Invoke(command.CommandText);

                throw;
            }
            finally
            {
                await connection.CloseAsync();
                await connection.DisposeAsync();
                await command.DisposeAsync();
                await transaction.DisposeAsync();
            }
        }

        public void OperateCollection<T>(IEnumerable<T> collection, Action<DbCommand> commandInitializer, Action<DbCommand, T> bindingAction, IsolationLevel isolationLevel, Action<T> onItemFail)
        {
            if (collection.FirstOrDefault() == null) return;

            DbConnection connection = _provider.CreateConnection();
            DbCommand command = connection.CreateCommand();

            connection.Open();
            
            DbTransaction transaction = connection.BeginTransaction(isolationLevel);

            command.Transaction = transaction;

            T[] copy = collection.ToArray();

            int count = copy.Length;

            T current = default!;

            try
            {
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
            catch
            {
                transaction.Rollback();

                onItemFail.Invoke(current);

                throw;
            }
            finally
            {
                connection.Close();
                connection.Dispose();
                command.Dispose();
                transaction.Dispose();
            }
        }

        public async Task OperateCollectionAsync<T>(IEnumerable<T> collection, Action<DbCommand> commandInitializer, Action<DbCommand, T> bindingAction, IsolationLevel isolationLevel, Action<T> onItemFail, CancellationToken cancellationToken = default)
        {
            if (!collection.Any()) return;

            DbConnection connection = _provider.CreateConnection();
            DbCommand command = connection.CreateCommand();

            await connection.OpenAsync(cancellationToken);

            DbTransaction transaction = await connection.BeginTransactionAsync(isolationLevel, cancellationToken);

            command.Transaction = transaction;

            T[] copy = collection.ToArray();

            int count = copy.Length;

            T current = default!;

            try
            {
                commandInitializer.Invoke(command);

                for (int i = 0; i != count; ++i)
                {
                    current = copy[i];
                    bindingAction.Invoke(command, current);
                    await command.ExecuteNonQueryAsync(cancellationToken);
                    command.Parameters.Clear();
                }

                await transaction.CommitAsync(cancellationToken);
            }
            catch
            {
                if(transaction != null)
                {
                    await transaction.RollbackAsync(cancellationToken);
                }

                onItemFail.Invoke(current);

                throw;
            }
            finally
            {
                await connection.CloseAsync();
                await connection.DisposeAsync();
                await command.DisposeAsync();
                await transaction.DisposeAsync();
            }
        }

        public SqlTransaction CreateScopedTransaction(IsolationLevel isolationLevel)
        {
            return new SqlTransaction(
                connection: _provider.CreateOpenedConnection(),
                isolationLevel: isolationLevel);
        }

        public IEnumerable<T> Get<T>(Func<IDataReader, List<T>> mappingMethod, string query)
        {
            using var command = _provider.CreateCommand(query);
            return Get(mappingMethod, command);
        }

        public IEnumerable<T> Get<T>(Func<IDataReader, List<T>> mappingMethod, DbCommand command)
        {
            using (DbConnection connection = _provider.CreateConnection())
            {
                command.Connection = connection;

                try
                {
                    command.Connection.Open();

                    return mappingMethod.Invoke(command.ExecuteReader());
                }
                finally
                {
                    command.Connection.Close();
                }
            }
        }

        public IEnumerable<T> Get<T>(IDataMapper<T> dataMapper, DbCommand command) where T : class, new()
        {
            DbConnection connection = command.Connection ??= _provider.CreateConnection();

            connection.Open();

            IDataReader reader = command.ExecuteReader();

            try
            {
                while (reader.Read()) yield return dataMapper.CreateMappedInstance(reader);

                reader.Close();
            }
            finally
            {
                connection.Close();
                command.Connection = null;
                connection.Dispose();
            }
        }

        public IEnumerable<T> Get<T>(IDataMapper<T> dataMapper, string query) where T : class, new()
        {
            using var command = _provider.CreateCommand(query);
            return Get(dataMapper: dataMapper, command: command);
        }

        public IEnumerable<T> Get<T>(DbCommand command) where T : class, new()
        {
            return Get(dataMapper: new ReflectionDataMapper<T>(), command: command);
        }

        public IEnumerable<T> Get<T>(string query) where T : class, new()
        {
            using var command = _provider.CreateCommand(query);
            return Get<T>(command);
        }

        public async Task<IEnumerable<T>> GetAsync<T>(IDataMapper<T> dataMapper, DbCommand command, CancellationToken cancellationToken = default) where T : class, new()
        {
            DbConnection connection = command.Connection ??= _provider.CreateConnection();
            try
            {
                List<T> temp = new();

                await connection.OpenAsync(cancellationToken);

                using DbDataReader reader = await command.ExecuteReaderAsync(cancellationToken);
                while (await reader.ReadAsync(cancellationToken)) temp.Add(dataMapper.CreateMappedInstance(reader));

                return temp;
            }
            finally
            {
                command.Connection = null;
                await connection.CloseAsync();
                await connection.DisposeAsync();
            }
        }

        public async Task<IEnumerable<T>> GetAsync<T>(IDataMapper<T> dataMapper, string query, CancellationToken cancellationToken = default) where T : class, new()
        {
            using var command = _provider.CreateCommand(query);
            return await GetAsync(dataMapper, command, cancellationToken);
        }

        public async Task<IEnumerable<T>> GetAsync<T>(IDataMapper<T> dataMapper, DbCommand command) where T : class, new()
        {
            return await GetAsync(dataMapper, command, CancellationToken.None);
        }

        public async Task<IEnumerable<T>> GetAsync<T>(IDataMapper<T> dataMapper, string query) where T : class, new()
        {
            using var command = _provider.CreateCommand(query);
            return await GetAsync(dataMapper, command);
        }

        public async Task<IEnumerable<T>> GetAsync<T>(DbCommand command) where T : class, new()
        {
            return await GetAsync(new ReflectionDataMapper<T>(), command);
        }

        public async Task<IEnumerable<T>> GetAsync<T>(string query) where T : class, new()
        {
            using var command = _provider.CreateCommand(query);
            return await GetAsync<T>(command);
        }
        
        public async IAsyncEnumerable<T> GetAsyncEnumerable<T>(IDataMapper<T> dataMapper, DbCommand command, [EnumeratorCancellation] CancellationToken cancellationToken = default) where T : class, new()
        {
            DbConnection connection = command.Connection ??= _provider.CreateConnection();
            try
            {
                await connection.OpenAsync(cancellationToken);

                using DbDataReader reader = await command.ExecuteReaderAsync(cancellationToken);

                while (await reader.ReadAsync(cancellationToken)) yield return dataMapper.CreateMappedInstance(reader);

                await reader.CloseAsync();
            }
            finally
            {
                command.Connection = null;
                await connection.CloseAsync();
                await connection.DisposeAsync();
            }
        }

        public IAsyncEnumerable<T> GetAsyncEnumerable<T>(IDataMapper<T> dataMapper, string query, CancellationToken cancellationToken = default) where T : class, new()
        {
            using var command = _provider.CreateCommand(query);
            return GetAsyncEnumerable(dataMapper, command, cancellationToken);
        }

        public IAsyncEnumerable<T> GetAsyncEnumerable<T>(IDataMapper<T> dataMapper, DbCommand command) where T : class, new()
        {
            return GetAsyncEnumerable(dataMapper, command, CancellationToken.None);
        }

        public IAsyncEnumerable<T> GetAsyncEnumerable<T>(IDataMapper<T> dataMapper, string query) where T : class, new()
        {
            using var command = _provider.CreateCommand(query);
            return GetAsyncEnumerable(dataMapper, command);
        }

        public IAsyncEnumerable<T> GetAsyncEnumerable<T>(DbCommand command) where T : class, new()
        {
            return GetAsyncEnumerable(new ReflectionDataMapper<T>(), command);
        }

        public IAsyncEnumerable<T> GetAsyncEnumerable<T>(string query) where T : class, new()
        {
            using var command = _provider.CreateCommand(query);
            return GetAsyncEnumerable<T>(command);
        }
        
        public IEnumerable<dynamic> GetDynamic(DbCommand command)
        {
            throw new NotImplementedException();
        }

        public IEnumerable<dynamic> GetDynamic(string commandString)
        {
            throw new NotImplementedException();
        }

        public Task<IEnumerable<dynamic>> GetDynamicAsync(DbCommand command, CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException();
        }

        public void Iterate<T>(IDataMapper<T> dataMapper, Action<T> iteratorAction, DbCommand command) where T : class, new()
        {
            DbConnection connection = command.Connection ??= _provider.CreateConnection();
            try
            {
                connection.Open();

                using DbDataReader reader = command.ExecuteReader();
                while (reader.Read()) iteratorAction.Invoke(dataMapper.CreateMappedInstance(reader));
            }
            finally
            {
                command.Connection = null;
                connection.Close();
                connection.Dispose();
            }
        }

        public void Iterate<T>(IDataMapper<T> dataMapper, Action<T> iteratorAction, string query) where T : class, new()
        {
            using var command = _provider.CreateCommand(query);
            Iterate(dataMapper, iteratorAction, command);
        }

        public async Task IterateAsync<T>(IDataMapper<T> dataMapper, Action<T> iteratorAction, DbCommand command, CancellationToken cancellationToken = default) where T : class, new()
        {
            DbConnection connection = command.Connection ??= _provider.CreateConnection();
            try
            {
                await connection.OpenAsync(cancellationToken);

                using DbDataReader reader = await command.ExecuteReaderAsync(cancellationToken);

                while (await reader.ReadAsync(cancellationToken))
                {
                    iteratorAction.Invoke(dataMapper.CreateMappedInstance(reader));
                }
            }
            finally
            { 
                command.Connection = null;
                await connection.CloseAsync();
                await connection.DisposeAsync();
            }
        }

        public async Task IterateAsync<T>(IDataMapper<T> dataMapper, Action<T> iteratorAction, string query, CancellationToken cancellationToken = default) where T : class, new()
        {
            using var command = _provider.CreateCommand(query);
            await IterateAsync(dataMapper, iteratorAction, command, cancellationToken);
        }
    }
}
