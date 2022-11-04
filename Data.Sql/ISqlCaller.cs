using Data.Sql.Mapping;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;

namespace Data.Sql
{
    public interface ISqlCaller
    {
        DataTable Query(DbCommand command);
        DataTable Query(string queryString);
        Task<DataTable> QueryAsync(DbCommand command, CancellationToken cancellationToken = default);
        Task<DataTable> QueryAsync(string queryString, CancellationToken cancellationToken = default);
        DataTable? GetSchema(string queryString);
        Task<DataTable?> GetSchemaAsync(string queryString, CancellationToken cancellationToken = default);
        int ExecuteNonQuery(DbCommand command);
        int ExecuteNonQuery(string commandString);
        Task<int> ExecuteNonQueryAsync(DbCommand command, CancellationToken cancellationToken = default);
        Task<int> ExecuteNonQueryAsync(string commandString, CancellationToken cancellationToken = default);
        object? ExecuteScalar(DbCommand command);
        object? ExecuteScalar(string queryString);
        Task<object?> ExecuteScalarAsync(DbCommand command, CancellationToken cancellationToken = default);
        Task<object?> ExecuteScalarAsync(string queryString, CancellationToken cancellationToken = default);
        void Transact(IsolationLevel isolationLevel, Queue<Action<DbCommand>> commandActions, Action<string> onCommandFailed);
        Task TransactAsync(IsolationLevel isolationLevel, Queue<Action<DbCommand>> commandActions, Action<string> onCommandFailed, CancellationToken cancellationToken = default);
        void OperateCollection<T>(IEnumerable<T> collection, Action<DbCommand> commandInitializer, Action<DbCommand, T> bindingAction, IsolationLevel isolationLevel, Action<T> onItemFail);
        Task OperateCollectionAsync<T>(IEnumerable<T> collection, Action<DbCommand> commandInitializer, Action<DbCommand, T> bindingAction, IsolationLevel isolationLevel, Action<T> onItemFail, CancellationToken cancellationToken = default);
        SqlTransaction CreateScopedTransaction(IsolationLevel isolationLevel);
        IEnumerable<T> Get<T>(Func<IDataReader, List<T>> mappingMethod, string query);
        IEnumerable<T> Get<T>(Func<IDataReader, List<T>> mappingMethod, DbCommand command);
        IEnumerable<T> Get<T>(IDataMapper<T> dataMapper, DbCommand command) where T : class, new();
        IEnumerable<T> Get<T>(IDataMapper<T> dataMapper, string query) where T : class, new();
        IEnumerable<T> Get<T>(DbCommand command) where T : class, new();
        IEnumerable<T> Get<T>(string query) where T : class, new();
        Task<IEnumerable<T>> GetAsync<T>(IDataMapper<T> dataMapper, DbCommand command, CancellationToken cancellationToken = default) where T : class, new();
        Task<IEnumerable<T>> GetAsync<T>(IDataMapper<T> dataMapper, string query, CancellationToken cancellationToken = default) where T : class, new();
        Task<IEnumerable<T>> GetAsync<T>(IDataMapper<T> dataMapper, DbCommand command) where T : class, new();
        Task<IEnumerable<T>> GetAsync<T>(IDataMapper<T> dataMapper, string query) where T : class, new();
        Task<IEnumerable<T>> GetAsync<T>(DbCommand command) where T : class, new();
        Task<IEnumerable<T>> GetAsync<T>(string query) where T : class, new();
        IAsyncEnumerable<T> GetAsyncEnumerable<T>(IDataMapper<T> dataMapper, DbCommand command, CancellationToken cancellationToken = default) where T : class, new();
        IAsyncEnumerable<T> GetAsyncEnumerable<T>(IDataMapper<T> dataMapper, string query, CancellationToken cancellationToken = default) where T : class, new();
        IAsyncEnumerable<T> GetAsyncEnumerable<T>(IDataMapper<T> dataMapper, DbCommand command) where T : class, new();
        IAsyncEnumerable<T> GetAsyncEnumerable<T>(IDataMapper<T> dataMapper, string query) where T : class, new();
        IAsyncEnumerable<T> GetAsyncEnumerable<T>(DbCommand command) where T : class, new();
        IAsyncEnumerable<T> GetAsyncEnumerable<T>(string query) where T : class, new();
        void Iterate<T>(IDataMapper<T> dataMapper, Action<T> iteratorAction, DbCommand command) where T : class, new();
        void Iterate<T>(IDataMapper<T> dataMapper, Action<T> iteratorAction, string query) where T : class, new();
        Task IterateAsync<T>(IDataMapper<T> dataMapper, Action<T> iteratorAction, DbCommand command, CancellationToken cancellationToken = default) where T : class, new();
        Task IterateAsync<T>(IDataMapper<T> dataMapper, Action<T> iteratorAction, string query, CancellationToken cancellationToken = default) where T : class, new();
        IEnumerable<dynamic> GetDynamic(string commandString);
        IEnumerable<dynamic> GetDynamic(DbCommand command);
        Task<IEnumerable<dynamic>> GetDynamicAsync(DbCommand command, CancellationToken cancellationToken = default);
    }
}
