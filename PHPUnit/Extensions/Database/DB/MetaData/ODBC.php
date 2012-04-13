<?php
/**
 * PHPUnit
 *
 * Copyright (c) 2002-2010, Sebastian Bergmann <sb@sebastian-bergmann.de>.
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
 * Wrapper around the PDO object to be able to specify information about the underlying driver
 *
 * @package    DbUnit
 * @author     Patrick Webster <noxwizard@gmail.com>
 * @copyright  2002-2011 Sebastian Bergmann <sb@sebastian-bergmann.de>
 * @license    http://www.opensource.org/licenses/bsd-license.php  BSD License
 * @version    Release: @package_version@
 * @link       http://www.phpunit.de/
 * @since      Class available since Release 1.1.0
 */
if(!class_exists('phpbb_database_connection_odbc_pdo_wrapper'))
{
	class phpbb_database_connection_odbc_pdo_wrapper extends PDO
	{
		// Name of the driver being used (i.e. mssql, firebird)
		public $driver = '';

		// Version number of driver since PDO::getAttribute(PDO::ATTR_CLIENT_VERSION) is pretty useless for this
		public $version = 0;

		function __construct($dbms, $version, $dsn, $user, $pass)
		{
			$this->driver = $dbms;
			$this->version = (double)$version;

			parent::__construct($dsn, $user, $pass);
		}
	}
}

/**
 * Provides functionality to retrieve meta data from an ODBC connection for the following database drivers:
 *   Firebird
 *   Microsoft SQL Server
 *
 * @package    DbUnit
 * @author     Patrick Webster <noxwizard@phpbb.com>
 * @copyright  2002-2011 Sebastian Bergmann <sb@sebastian-bergmann.de>
 * @license    http://www.opensource.org/licenses/bsd-license.php  BSD License
 * @version    Release: @package_version@
 * @link       http://www.phpunit.de/
 * @since      -
 */
class PHPUnit_Extensions_Database_DB_MetaData_ODBC extends PHPUnit_Extensions_Database_DB_MetaData
{
	/**
     * The name of the underlying database system we are connecting to.
     * @var string
     */
	protected $realDriver = '';

	/**
     * The ODBC driver version
     * @var string
     */
	protected $version = 0;

	/**
     * Constructor
     * @var phpbb_database_connection_odbc_pdo_wrapper
	 * @var string - schema
     */
	function __construct($args)
	{
		parent::__construct($args);

		$pdo_obj = func_get_arg(0);
		if(!($pdo_obj instanceof phpbb_database_connection_odbc_pdo_wrapper))
		{
			throw new Exception('ODBC MetaDriver requires a phpbb_database_connection_odbc_pdo_wrapper PDO object and not a regular PDO object.');
		}
		
		$this->realDriver = strtolower($pdo_obj->driver);
		$this->version = (double)$pdo_obj->version;
		
		switch($this->realDriver)
		{
			case 'firebird':
				$this->schemaObjectQuoteChar = '"';
				$this->truncateCommand       = 'DELETE FROM';
			break;
			
			case 'mssql':
				$this->schemaObjectQuoteChar = '';
				$this->truncateCommand       = 'TRUNCATE TABLE';
			break;
			
			default:
				throw new Exception('The ODBC MetaDriver does not support your underlying DBMS: ' . $this->realDriver);
			break;
		}
	}

    /**
     * Returns an array containing the names of all the tables in the database.
     *
     * @return array
     */
    public function getTableNames()
    {
		switch($this->realDriver)
		{
			case 'firebird':
				$query = 'SELECT RDB$RELATION_NAME
							FROM RDB$RELATIONS
							WHERE RDB$VIEW_SOURCE is null
								AND RDB$SYSTEM_FLAG = 0';
			break;
			
			case 'mssql':
				$query = "SELECT name
							FROM sysobjects
							WHERE type='U'";
			break;
		}
		
		$statement = $this->pdo->query($query);

		$tableNames = array();
		while (($tableName = $statement->fetchColumn(0))) {
			$tableNames[] = $tableName;
		}

        return $tableNames;
    }

    /**
     * Returns an array containing the names of all the columns in the
     * $tableName table.
     *
     * @param string $tableName
     * @return array
     */
    public function getTableColumns($tableName)
    {
		switch($this->realDriver)
		{
			case 'firebird':
				$query = 'SELECT RDB$FIELD_NAME
							FROM RDB$RELATION_FIELDS
							WHERE RDB$RELATION_NAME = \'' . strtoupper($tableName) . "'";
			break;

			case 'mssql':
				$query = "SELECT c.name
							FROM syscolumns c
					   LEFT JOIN sysobjects o ON c.id = o.id
						   WHERE o.name = '$tableName'";
			break;
		}
		
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
		switch($this->realDriver)
		{
			case 'firebird':
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
			break;

			case 'mssql':
				$query     = "EXEC sp_statistics '$tableName'";
				$statement = $this->pdo->prepare($query);
				$statement->execute();
				$statement->setFetchMode(PDO::FETCH_ASSOC);

				$columnNames = array();
				while (($column = $statement->fetch())) {
					if ($column['TYPE'] == 1) {
						$columnNames[] = $column['COLUMN_NAME'];
					}
				}
			break;
		}

        return $columnNames;
    }

    /**
    * Allow overwriting identities for the given table.
    *
    * @param string $tableName
    */
    public function disablePrimaryKeys($tableName)
    {
		switch($this->realDriver)
		{
			case 'mssql':
				try {
					$query = "SET IDENTITY_INSERT $tableName ON";
					$this->pdo->exec($query);
				}
				catch (PDOException $e) {
					// ignore the error here - can happen if primary key is not an identity
				}
			break;
		}
    }

    /**
    * Reenable auto creation of identities for the given table.
    *
    * @param string $tableName
    */
    public function enablePrimaryKeys($tableName)
    {
		switch($this->realDriver)
		{
			case 'mssql':
				try {
					$query = "SET IDENTITY_INSERT $tableName OFF";
					$this->pdo->exec($query);
				}
				catch (PDOException $e) {
					// ignore the error here - can happen if primary key is not an identity
				}
			break;
		}
    }
}
