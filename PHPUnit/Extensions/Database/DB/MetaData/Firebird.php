<?php
/**
 * PHPUnit
 *
 * Copyright (c) 2002-2011, Sebastian Bergmann <sb@sebastian-bergmann.de>.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *
 *   * Redistributions of source code must retain the above copyright
 *     notice, this list of conditions and the following disclaimer.
 *
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in
 *     the documentation and/or other materials provided with the
 *     distribution.
 *
 *   * Neither the name of Sebastian Bergmann nor the names of his
 *     contributors may be used to endorse or promote products derived
 *     from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS
 * FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE
 * COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
 * INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
 * BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
 * CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN
 * ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 *
 * @package    DbUnit
 * @author     Patrick Webster <noxwizard@phpbb.com>
 * @copyright  2002-2011 Sebastian Bergmann <sb@sebastian-bergmann.de>
 * @license    http://www.opensource.org/licenses/bsd-license.php  BSD License
 * @link       http://www.phpunit.de/
 * @since      -
 */

/**
 * Provides functionality to retrieve meta data from a Firebird database.
 *
 * @package    DbUnit
 * @author     Patrick Webster <noxwizard@phpbb.com>
 * @copyright  2002-2011 Sebastian Bergmann <sb@sebastian-bergmann.de>
 * @license    http://www.opensource.org/licenses/bsd-license.php  BSD License
 * @version    Release: @package_version@
 * @link       http://www.phpunit.de/
 * @since      -
 */
class PHPUnit_Extensions_Database_DB_MetaData_Firebird extends PHPUnit_Extensions_Database_DB_MetaData
{
	protected $truncateCommand = 'DELETE FROM';

    /**
     * Returns an array containing the names of all the tables in the database.
     *
     * @return array
     */
    public function getTableNames()
    {
        $query = 'SELECT RDB$RELATION_NAME
					FROM RDB$RELATIONS
					WHERE RDB$VIEW_SOURCE is null
						AND RDB$SYSTEM_FLAG = 0';

        $result = $this->pdo->query($query);

        $tableNames = array();
        while ($tableName = $result->fetchColumn(0)) {
            $tableNames[] = $tableName;
        }

        return $tableNames;
    }

    /**
     * Returns an array containing the names of all the columns in the
     * $tableName table,
     *
     * @param string $tableName
     * @return array
     */
    public function getTableColumns($tableName)
    {
        $query = 'SELECT RDB$FIELD_NAME
					FROM RDB$RELATION_FIELDS
					WHERE RDB$RELATION_NAME = \'' . strtoupper($tableName) . "'";

		$result = $this->pdo->query($query);

        $columnNames = array();
        while (($columnName = $result->fetchColumn(0))) {
            $columnNames[] = $columnName;
        }

        return $columnNames;
    }

    /**
     * Returns an array containing the names of all the primary key columns in
     * the $tableName table.
     *
     * @param string $tableName
     * @return array
     */
    public function getTablePrimaryKeys($tableName)
    {
        $query = "SELECT RDB\$FIELD_NAME FROM RDB\$INDICES i
					LEFT JOIN RDB\$INDEX_SEGMENTS i2 ON (i2.RDB\$INDEX_NAME = i.RDB\$INDEX_NAME)
					LEFT JOIN RDB\$RELATION_CONSTRAINTS c ON (c.RDB\$INDEX_NAME = i2.RDB\$INDEX_NAME)
						WHERE c.RDB\$CONSTRAINT_TYPE = 'PRIMARY KEY'
							AND i.RDB\$RELATION_NAME = '" . strtoupper($tableName) . "'";

		$result = $this->pdo->query($query);

        $columnNames = array();
        while (($columnName = $result->fetchColumn(0))) {
			$columnNames[] = $columnName;
        }

        return $columnNames;
    }
}
