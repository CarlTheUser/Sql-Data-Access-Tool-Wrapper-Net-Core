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
        DataTable GetSchema(string queryString);
        int ExecuteNonQuery(DbCommand command);
        int ExecuteNonQuery(string commandString);
        object ExecuteScalar(DbCommand command);
        object ExecuteScalar(string queryString);
        void Transact(IsolationLevel isolationLevel, Queue<Action<DbCommand>> commandActions, Action<string> onCommandFailed);
        void OperateCollection<T>(IEnumerable<T> collection, Action<DbCommand> commandInitializer, Action<DbCommand, T> bindingAction, IsolationLevel isolationLevel, Action<T> onItemFail);
        SqlTransaction CreateScopedTransaction(IsolationLevel isolationLevel);
        IEnumerable<T> Get<T>(Func<IDataReader, List<T>> mappingMethod, string query);
        IEnumerable<T> Get<T>(Func<IDataReader, List<T>> mappingMethod, DbCommand command);
        IEnumerable<T> Get<T>(IDataMapper<T> dataMapper, DbCommand command) where T : class, new();
        IEnumerable<T> Get<T>(IDataMapper<T> dataMapper, string query) where T : class, new();
        IEnumerable<T> Get<T>(DbCommand command) where T : class, new();
        IEnumerable<T> Get<T>(string query) where T : class, new();
        Task<IEnumerable<T>> GetAsync<T>(IDataMapper<T> dataMapper, DbCommand command, CancellationToken token) where T : class, new();
        Task<IEnumerable<T>> GetAsync<T>(IDataMapper<T> dataMapper, string query, CancellationToken token) where T : class, new();
        Task<IEnumerable<T>> GetAsync<T>(IDataMapper<T> dataMapper, DbCommand command) where T : class, new();
        Task<IEnumerable<T>> GetAsync<T>(IDataMapper<T> dataMapper, string query) where T : class, new();
        Task<IEnumerable<T>> GetAsync<T>(DbCommand command) where T : class, new();
        Task<IEnumerable<T>> GetAsync<T>(string query) where T : class, new();
        void Iterate<T>(IDataMapper<T> dataMapper, Action<T> iteratorAction, DbCommand command) where T : class, new();
        void Iterate<T>(IDataMapper<T> dataMapper, Action<T> iteratorAction, string query) where T : class, new();
        Task IterateAsync<T>(IDataMapper<T> dataMapper, Action<T> iteratorAction, DbCommand command, CancellationToken token) where T : class, new();
        IEnumerable<dynamic> GetDynamic(string commandString);
        IEnumerable<dynamic> GetDynamic(DbCommand command);
        Task<IEnumerable<dynamic>> GetDynamicAsync(DbCommand command, CancellationToken token);
    }
}
