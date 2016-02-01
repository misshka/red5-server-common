/*
 * RED5 Open Source Flash Server - https://github.com/Red5/
 * 
 * Copyright 2006-2016 by respective authors (see below). All rights reserved.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.red5.server.api.stream;

import java.io.File;
import java.util.Set;

import org.red5.server.api.scope.IScopeService;
import org.red5.server.api.service.IStreamableFileService;

/**
 * Scope service extension that provides method to get streamable file services set
 */
public interface IStreamableFileFactory extends IScopeService {

    public static String BEAN_NAME = "streamableFileFactory";

    public abstract IStreamableFileService getService(File fp);

    /**
     * Getter for services
     *
     * @return Set of streamable file services
     */
    public abstract Set<IStreamableFileService> getServices();

}