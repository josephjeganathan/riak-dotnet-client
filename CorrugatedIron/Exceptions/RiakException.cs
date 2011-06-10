﻿// Copyright (c) 2010 - OJ Reeves & Jeremiah Peschka
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

using System;
using System.Collections;
using System.Collections.Generic;
using CorrugatedIron.Extensions;

namespace CorrugatedIron.Exceptions
{
    public class RiakException : Exception
    {
        public uint ErrorCode { get; private set; }
        public string ErrorMessage { get; set; }

        public RiakException(uint errorCode, string errorMessage)
        {
            ErrorCode = errorCode;
            ErrorMessage = errorMessage;
        }

        public override string Message
        {
            get
            {
                return "Riak returned an error. Code '{0}'. Message: {1}".Fmt(ErrorCode, ErrorMessage);
            }
        }

        public override IDictionary Data
        {
            get
            {
                return new Dictionary<string, object>
                {
                    { "ErrorCode", ErrorCode },
                    { "ErrorMessage", ErrorMessage }
                };
            }
        }
    }
}
