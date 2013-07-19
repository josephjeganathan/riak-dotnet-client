﻿// Copyright (c) 2013 - OJ Reeves & Jeremiah Peschka
//
// This file is provided to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file
// except in compliance with the License.  You may obtain
// a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

using System.Threading.Tasks;
using CorrugatedIron.Config;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using CorrugatedIron.Extensions;

namespace CorrugatedIron.Comms
{
    internal class RiakConnectionPool : IRiakConnectionManager
    {
        private readonly List<IRiakConnection> _allResources;
        private readonly ConcurrentStack<IRiakConnection> _resources;
        private bool _disposing;

        public RiakConnectionPool(IRiakNodeConfiguration nodeConfig, IRiakConnectionFactory connFactory)
        {
            var poolSize = nodeConfig.PoolSize;
            _allResources = new List<IRiakConnection>();
            _resources = new ConcurrentStack<IRiakConnection>();

            for(var i = 0; i < poolSize; ++i)
            {
                var conn = connFactory.CreateConnection(nodeConfig);
                _allResources.Add(conn);
                _resources.Push(conn);
            }
        }

        public Task<Tuple<bool, TResult>> Consume<TResult>(Func<IRiakConnection, Task<TResult>> consumer)
        {
            if(_disposing) return Tuple.Create(false, default(TResult)).ToTask();

            IRiakConnection instance = null;
            if(_resources.TryPop(out instance))

            {
                return consumer(instance)
                    .ContinueWith(t =>
                        {
                            if (instance != null)
                            {
                                _resources.Push(instance);
                            }

                            return t.IsFaulted
                                ? Tuple.Create(false, default(TResult))
                                : Tuple.Create(true, t.Result);
                        });
            }

            return Tuple.Create(false, default(TResult)).ToTask();
        }

        public void Dispose()
        {
            if(_disposing) return;

            _disposing = true;

            foreach(var conn in _allResources)
            {
                conn.Dispose();
            }
        }
    }
}
