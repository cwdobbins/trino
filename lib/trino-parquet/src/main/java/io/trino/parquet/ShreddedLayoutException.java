/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.parquet;

import io.trino.spi.TrinoException;

import java.util.Optional;

import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.util.Objects.requireNonNull;

/**
 * A shredded VARIANT layout this reader cannot read. The message names the Parquet column path and, once known, the file:
 * layouts differ from file to file, so the file is what someone debugging a writer needs. The file is added by whoever
 * knows it ({@link #inFile}), which for a layout refused while the reader is being built is the connector.
 */
public final class ShreddedLayoutException
        extends TrinoException
{
    private final String columnPath;
    private final String detail;
    private final Optional<String> file;

    public ShreddedLayoutException(String columnPath, String detail, Throwable cause)
    {
        this(columnPath, detail, Optional.empty(), cause);
    }

    private ShreddedLayoutException(String columnPath, String detail, Optional<String> file, Throwable cause)
    {
        super(NOT_SUPPORTED, message(columnPath, detail, file), cause);
        this.columnPath = requireNonNull(columnPath, "columnPath is null");
        this.detail = requireNonNull(detail, "detail is null");
        this.file = requireNonNull(file, "file is null");
    }

    private static String message(String columnPath, String detail, Optional<String> file)
    {
        String message = "Unsupported shredded VARIANT layout in column %s: %s".formatted(columnPath, detail);
        return file.map(name -> message + " (file " + name + ")").orElse(message);
    }

    public String getColumnPath()
    {
        return columnPath;
    }

    public Optional<String> getFile()
    {
        return file;
    }

    /**
     * This error naming the file, with the same cause and stack trace; itself when it names a file already.
     */
    public ShreddedLayoutException inFile(String file)
    {
        requireNonNull(file, "file is null");
        if (this.file.isPresent()) {
            return this;
        }
        ShreddedLayoutException withFile = new ShreddedLayoutException(columnPath, detail, Optional.of(file), getCause());
        withFile.setStackTrace(getStackTrace());
        for (Throwable suppressed : getSuppressed()) {
            withFile.addSuppressed(suppressed);
        }
        return withFile;
    }
}
