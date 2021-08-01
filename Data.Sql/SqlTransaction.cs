using Data.Sql.Mapping;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;

namespace Data.Sql
{
    public class SqlTransaction : IDisposable
    {
        private bool disposed = false;

        private bool transactionFinished = false;

        private readonly DbTransaction _dbTransaction;

        public SqlTransaction(DbConnection connection, IsolationLevel isolationLevel)
        {
            if (connection.State != ConnectionState.Open) connection.Open();

            _dbTransaction = connection.BeginTransaction(isolationLevel);
        }

        public int ExecuteNonQuery(DbCommand command)
        {
            DisposeCheck();

            DbConnection connection = _dbTransaction.Connection;

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

            DbConnection connection = _dbTransaction.Connection;

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

        public object ExecuteScalar(DbCommand command)
        {
            DisposeCheck();

            DbConnection connection = _dbTransaction.Connection;

            command.Connection = connection;

            command.Transaction = _dbTransaction;

            object returnValue = command.ExecuteScalar();

            command.Transaction = null;

            command.Connection = null;

            return returnValue;
        }

        public object ExecuteScalar(string query)
        {
            DisposeCheck();

            DbConnection connection = _dbTransaction.Connection;

            using (DbCommand command = connection.CreateCommand())
            {
                command.Transaction = _dbTransaction;

                command.CommandText = query;

                object returnValue = command.ExecuteScalar();

                command.Transaction = null;

                command.Connection = null;

                return returnValue;
            }
        }

        public IEnumerable<T> Get<T>(Func<IDataReader, List<T>> mappingMethod, string query)
        {
            DisposeCheck();

            if (mappingMethod == null) throw new ArgumentNullException(nameof(mappingMethod));

            DbConnection connection = _dbTransaction.Connection;

            DbCommand command = connection.CreateCommand();

            command.Transaction = _dbTransaction;

            command.CommandText = query;

            List<T> temp;

            IDataReader reader = null;

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

            if (mappingMethod == null) throw new ArgumentNullException(nameof(mappingMethod));

            DbConnection connection = _dbTransaction.Connection;

            command.Connection = connection;

            command.Transaction = _dbTransaction;

            List<T> temp;

            IDataReader reader = null;

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

            DbConnection connection = _dbTransaction.Connection;

            List<T> temp = new List<T>();

            command.Connection = connection;

            command.Transaction = _dbTransaction;

            IDataReader reader = null;

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

            List<T> temp = new List<T>();

            DbConnection connection = _dbTransaction.Connection;

            DbCommand command = connection.CreateCommand();

            command.CommandText = query;

            command.Transaction = _dbTransaction;

            IDataReader reader = null;

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

            return Get(new ReflectionDataMapper<T>(), command);
        }

        public IEnumerable<T> Get<T>(string query) where T : class, new()
        {
            DisposeCheck();

            List<T> temp = new List<T>();

            IDataMapper<T> mapper = new ReflectionDataMapper<T>();

            DbConnection connection = _dbTransaction.Connection;

            DbCommand command = connection.CreateCommand();

            command.CommandText = query;

            command.Transaction = _dbTransaction;

            IDataReader reader = null;

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

        public void Iterate<T>(IDataMapper<T> dataMapper, Action<T> iteratorAction, DbCommand command) where T : class, new()
        {
            DisposeCheck();

            DbConnection connection = _dbTransaction.Connection;

            command.Connection = connection;

            command.Transaction = _dbTransaction;

            DbDataReader reader = null;

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

            DbConnection connection = _dbTransaction.Connection;

            DbCommand command = connection.CreateCommand();

            command.CommandText = query;

            command.Connection = connection;

            command.Transaction = _dbTransaction;

            DbDataReader reader = null;

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

        public void Commit()
        {
            DisposeCheck();

            _dbTransaction.Commit();

            transactionFinished = true;
        }

        public void Rollback()
        {
            DisposeCheck();

            _dbTransaction.Rollback();

            transactionFinished = true;
        }

        public void Dispose()
        {
            Dispose(true);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!disposed && disposing)
            {
                if (!transactionFinished)
                {
                    _dbTransaction.Commit();

                    transactionFinished = true;
                }

                _dbTransaction.Dispose();

                disposed = true;
            }
        }

        private void DisposeCheck()
        {
            if (disposed) throw new ObjectDisposedException(typeof(SqlTransaction).Name);
        }
    }
}
