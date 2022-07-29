using Data.Sql.Mapping;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;

namespace Data.Sql
{
    public class SqlTransaction : IDisposable
    {
        private bool _disposed = false;

        private bool _transactionCompleted = false;

        private readonly DbTransaction _dbTransaction;

        public SqlTransaction(DbConnection connection, IsolationLevel isolationLevel)
        {
            if (connection.State != ConnectionState.Open) connection.Open();

            _dbTransaction = connection.BeginTransaction(isolationLevel);
        }

        public int ExecuteNonQuery(DbCommand command)
        {
            DisposeCheck();

            DbConnection connection = _dbTransaction.Connection!;

            command.Connection = connection;

            command.Transaction = _dbTransaction;

            int affectedRows = command.ExecuteNonQuery();

            command.Transaction = null;

            command.Connection = null;

            return affectedRows;
        }

        public int ExecuteNonQuery(string query)
        {
            DisposeCheck();

            DbConnection connection = _dbTransaction.Connection!;

            using (DbCommand command = connection.CreateCommand())
            {
                command.Transaction = _dbTransaction;

                command.CommandText = query;

                int affectedRows = command.ExecuteNonQuery();

                command.Transaction = null;

                command.Connection = null;

                return affectedRows;
            }
        }

        public async Task<int> ExecuteNonQueryAsync(DbCommand command, CancellationToken token)
        {
            DisposeCheck();

            DbConnection connection = _dbTransaction.Connection!;

            command.Connection = connection;

            command.Transaction = _dbTransaction;

            int affectedRows = await command.ExecuteNonQueryAsync(token);

            command.Transaction = null;

            command.Connection = null;

            return affectedRows;
        }

        public async Task<int> ExecuteNonQueryAsync(string query, CancellationToken token)
        {
            DisposeCheck();

            DbConnection connection = _dbTransaction.Connection!;

            using DbCommand command = connection.CreateCommand();

            command.Transaction = _dbTransaction;

            command.CommandText = query;

            int affectedRows = await command.ExecuteNonQueryAsync(token);

            command.Transaction = null;

            command.Connection = null;

            return affectedRows;
        }

        public object? ExecuteScalar(DbCommand command)
        {
            DisposeCheck();

            DbConnection connection = _dbTransaction.Connection!;

            command.Connection = connection;

            command.Transaction = _dbTransaction;

            object? returnValue = command.ExecuteScalar();

            command.Transaction = null;

            command.Connection = null;

            return returnValue;
        }

        public object? ExecuteScalar(string query)
        {
            DisposeCheck();

            DbConnection connection = _dbTransaction.Connection!;

            using (DbCommand command = connection.CreateCommand())
            {
                command.Transaction = _dbTransaction;

                command.CommandText = query;

                object? returnValue = command.ExecuteScalar();

                command.Transaction = null;

                command.Connection = null;

                return returnValue;
            }
        }

        public async Task<object?> ExecuteScalarAsync(DbCommand command, CancellationToken token)
        {
            DisposeCheck();

            DbConnection connection = _dbTransaction.Connection!;

            command.Connection = connection;

            command.Transaction = _dbTransaction;

            object? returnValue = await command.ExecuteScalarAsync(token);

            command.Transaction = null;

            command.Connection = null;

            return returnValue;
        }

        public async Task<object?> ExecuteScalarAsync(string query, CancellationToken token)
        {
            DisposeCheck();

            DbConnection connection = _dbTransaction.Connection!;

            using (DbCommand command = connection.CreateCommand())
            {
                command.Transaction = _dbTransaction;

                command.CommandText = query;

                object? returnValue = await command.ExecuteScalarAsync(token);

                command.Transaction = null;

                command.Connection = null;

                return returnValue;
            }
        }

        public IEnumerable<T> Get<T>(Func<IDataReader, List<T>> mappingMethod, string query)
        {
            DisposeCheck();

            DbConnection connection = _dbTransaction.Connection!;

            DbCommand command = connection.CreateCommand();

            command.Transaction = _dbTransaction;

            command.CommandText = query;

            List<T> temp;

            IDataReader? reader = default;

            try
            {
                temp = mappingMethod.Invoke(reader = command.ExecuteReader());

                return temp;
            }
            finally
            {
                reader?.Dispose();

                command.Connection = null;

                command.Transaction = null;

                command.Dispose();
            }
        }

        public IEnumerable<T> Get<T>(Func<IDataReader, List<T>> mappingMethod, DbCommand command)
        {
            DisposeCheck();

            DbConnection connection = _dbTransaction.Connection!;

            command.Connection = connection;

            command.Transaction = _dbTransaction;

            List<T> temp;

            IDataReader? reader = null;

            try
            {
                temp = mappingMethod.Invoke(reader = command.ExecuteReader());

                return temp;
            }
            finally
            {
                reader?.Dispose();

                command.Connection = null;

                command.Transaction = null;

                command.Dispose();
            }
        }

        public IEnumerable<T> Get<T>(IDataMapper<T> dataMapper, DbCommand command) where T : class, new()
        {
            DisposeCheck();

            DbConnection connection = _dbTransaction.Connection!;

            List<T> temp = new();

            command.Connection = connection;

            command.Transaction = _dbTransaction;

            IDataReader? reader = null;

            try
            {
                reader = command.ExecuteReader();

                while (reader.Read()) temp.Add(dataMapper.CreateMappedInstance(reader));

                return temp;
            }
            finally
            {
                reader?.Dispose();

                command.Connection = null;

                command.Transaction = null;
            }
        }

        public IEnumerable<T> Get<T>(IDataMapper<T> dataMapper, string query) where T : class, new()
        {
            DisposeCheck();

            List<T> temp = new();

            DbConnection connection = _dbTransaction.Connection!;

            DbCommand command = connection.CreateCommand();

            command.CommandText = query;

            command.Transaction = _dbTransaction;

            IDataReader? reader = null;

            try
            {
                reader = command.ExecuteReader();

                while (reader.Read()) temp.Add(dataMapper.CreateMappedInstance(reader));

                return temp;
            }
            finally
            {
                reader?.Dispose();

                command.Connection = null;

                command.Transaction = null;

                command.Dispose();
            }
        }

        public IEnumerable<T> Get<T>(DbCommand command) where T : class, new()
        {
            DisposeCheck();

            return Get(dataMapper: new ReflectionDataMapper<T>(), command: command);
        }

        public IEnumerable<T> Get<T>(string query) where T : class, new()
        {
            DisposeCheck();

            List<T> temp = new();

            var mapper = new ReflectionDataMapper<T>();

            DbConnection connection = _dbTransaction.Connection!;

            DbCommand command = connection.CreateCommand();

            command.CommandText = query;

            command.Transaction = _dbTransaction;

            IDataReader? reader = null;

            try
            {
                reader = command.ExecuteReader();

                while (reader.Read()) temp.Add(mapper.CreateMappedInstance(reader));
            }
            finally
            {
                reader?.Dispose();

                command.Connection = null;

                command.Transaction = null;

                command.Dispose();
            }

            return temp;
        }

        public async Task<IEnumerable<T>> GetAsync<T>(IDataMapper<T> dataMapper, DbCommand command, CancellationToken token) where T : class, new()
        {
            DisposeCheck();

            DbConnection connection = _dbTransaction.Connection!;

            List<T> temp = new();

            command.Connection = connection;

            command.Transaction = _dbTransaction;

            DbDataReader? reader = null;

            try
            {
                reader = await command.ExecuteReaderAsync(token);

                while (await reader.ReadAsync(token)) temp.Add(dataMapper.CreateMappedInstance(reader));

                return temp;
            }
            finally
            {
                reader?.Dispose();

                command.Connection = null;

                command.Transaction = null;
            }
        }

        public async Task<IEnumerable<T>> GetAsync<T>(IDataMapper<T> dataMapper, string query, CancellationToken token) where T : class, new()
        {
            DisposeCheck();

            List<T> temp = new();

            DbConnection connection = _dbTransaction.Connection!;

            DbCommand command = connection.CreateCommand();

            command.CommandText = query;

            command.Transaction = _dbTransaction;

            DbDataReader? reader = null;

            try
            {
                reader = await command.ExecuteReaderAsync(token);

                while (await reader.ReadAsync(token)) temp.Add(dataMapper.CreateMappedInstance(reader));

                return temp;
            }
            finally
            {
                reader?.Dispose();

                command.Connection = null;

                command.Transaction = null;

                command.Dispose();
            }
        }

        public async Task<IEnumerable<T>> GetAsync<T>(DbCommand command, CancellationToken token) where T : class, new()
        {
            DisposeCheck();

            return await GetAsync(
                dataMapper: new ReflectionDataMapper<T>(),
                command: command, 
                token: token);
        }

        public async Task<IEnumerable<T>> GetAsync<T>(string query, CancellationToken token) where T : class, new()
        {
            DisposeCheck();

            List<T> temp = new();

            var mapper = new ReflectionDataMapper<T>();

            DbConnection connection = _dbTransaction.Connection!;

            DbCommand command = connection.CreateCommand();

            command.CommandText = query;

            command.Transaction = _dbTransaction;

            DbDataReader? reader = null;

            try
            {
                reader = await command.ExecuteReaderAsync(token);

                while (await reader.ReadAsync(token)) temp.Add(mapper.CreateMappedInstance(reader));
            }
            finally
            {
                reader?.Dispose();

                command.Connection = null;

                command.Transaction = null;

                command.Dispose();
            }

            return temp;
        }

        public void Iterate<T>(IDataMapper<T> dataMapper, Action<T> iteratorAction, DbCommand command) where T : class, new()
        {
            DisposeCheck();

            DbConnection connection = _dbTransaction.Connection!;

            command.Connection = connection;

            command.Transaction = _dbTransaction;

            DbDataReader? reader = null;

            try
            {
                reader = command.ExecuteReader();

                while (reader.Read()) iteratorAction.Invoke(dataMapper.CreateMappedInstance(reader));
            }
            finally
            {
                reader?.Dispose();

                command.Connection = null;

                command.Transaction = null;
            }
        }

        public void Iterate<T>(IDataMapper<T> dataMapper, Action<T> iteratorAction, string query) where T : class, new()
        {
            DisposeCheck();

            DbConnection connection = _dbTransaction.Connection!;

            DbCommand command = connection.CreateCommand();

            command.CommandText = query;

            command.Connection = connection;

            command.Transaction = _dbTransaction;

            DbDataReader? reader = null;

            try
            {
                reader = command.ExecuteReader();

                while (reader.Read()) iteratorAction.Invoke(dataMapper.CreateMappedInstance(reader));
            }
            finally
            {
                reader?.Dispose();

                command.Connection = null;

                command.Transaction = null;

                command.Dispose();
            }
        }

        public async Task IterateAsync<T>(IDataMapper<T> dataMapper, Action<T> iteratorAction, DbCommand command, CancellationToken token) where T : class, new()
        {
            DisposeCheck();

            DbConnection connection = _dbTransaction.Connection!;

            command.Connection = connection;

            command.Transaction = _dbTransaction;

            DbDataReader? reader = null;

            try
            {
                reader = await command.ExecuteReaderAsync(token);

                while (await reader.ReadAsync(token)) iteratorAction.Invoke(dataMapper.CreateMappedInstance(reader));
            }
            finally
            {
                reader?.Dispose();

                command.Connection = null;

                command.Transaction = null;
            }
        }

        public async Task IterateAsync<T>(IDataMapper<T> dataMapper, Action<T> iteratorAction, string query, CancellationToken token) where T : class, new()
        {
            DisposeCheck();

            DbConnection connection = _dbTransaction.Connection!;

            DbCommand command = connection.CreateCommand();

            command.CommandText = query;

            command.Connection = connection;

            command.Transaction = _dbTransaction;

            DbDataReader? reader = null;

            try
            {
                reader = await  command.ExecuteReaderAsync(token);

                while (await reader.ReadAsync(token)) iteratorAction.Invoke(dataMapper.CreateMappedInstance(reader));
            }
            finally
            {
                reader?.Dispose();

                command.Connection = null;

                command.Transaction = null;

                command.Dispose();
            }
        }

        public void Commit()
        {
            DisposeCheck();

            _dbTransaction.Commit();

            _transactionCompleted = true;
        }

        public async Task CommitAsync(CancellationToken token)
        {
            DisposeCheck();

            await _dbTransaction.CommitAsync(token);

            _transactionCompleted = true;
        }

        public void Rollback()
        {
            DisposeCheck();

            _dbTransaction.Rollback();

            _transactionCompleted = true;
        }

        public async Task RollbackAsync(CancellationToken token)
        {
            DisposeCheck();

            await _dbTransaction.RollbackAsync(token);

            _transactionCompleted = true;
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!_disposed && disposing)
            {
                if (!_transactionCompleted)
                {
                    _dbTransaction.Commit();

                    _transactionCompleted = true;
                }

                _dbTransaction.Dispose();

                _disposed = true;
            }
        }

        private void DisposeCheck()
        {
            if (_disposed) throw new ObjectDisposedException(typeof(SqlTransaction).Name);
        }
    }
}
