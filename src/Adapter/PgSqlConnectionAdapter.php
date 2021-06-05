<?php

namespace IKTO\PgSqlMigrationDirectories\Adapter;

use IKTO\PgSqlMigrationDirectories\Exception\QueryException;
use IKTO\PgSqlMigrationDirectories\Exception\TransactionException;
use IKTO\PgSqlMigrationDirectories\Helper\PgExceptionHelper;
use IKTO\PgMigrationDirectories\Adapter\ConnectionAdapterInterface;

class PgSqlConnectionAdapter implements ConnectionAdapterInterface
{
    protected $dbh;

    protected $savepointNames = [];
    protected $transactionStack = [];

    public function __construct($dbh)
    {
        $this->dbh = $dbh;
    }

    /**
     * {@inheritdoc}
     */
    public function openTransaction()
    {
        $status = pg_transaction_status($this->dbh);

        switch ($status) {
            case PGSQL_TRANSACTION_INTRANS:
            case PGSQL_TRANSACTION_INERROR:
                $name = $this->getSavepointName();
                try {
                    $this->pgQuery('SAVEPOINT "' . $name . '"');
                } catch (QueryException $ex) {
                    throw new TransactionException(sprintf('Cannot create savepoint %s', $name), null, $ex);
                }
                $this->savepointNames[$name] = 1;

                break;
            default:
                try {
                    $this->pgQuery('BEGIN');
                } catch (QueryException $ex) {
                    throw new TransactionException('Cannot start the transaction', null, $ex);
                }

                break;
        }
    }

    /**
     * {@inheritdoc}
     */
    public function commitTransaction()
    {
        if (count($this->transactionStack)) {
            $name = array_pop($this->transactionStack);
            try {
                $this->pgQuery('RELEASE SAVEPOINT "' . $name . '"');
            } catch (QueryException $ex) {
                throw new TransactionException(sprintf('Cannot release savepoint %s', $name), null, $ex);
            }

            unset($this->savepointNames[$name]);
        } else {
            try {
                $this->pgQuery('COMMIT');
            } catch (QueryException $ex) {
                throw new TransactionException('Cannot commit transaction', null, $ex);
            }
        }
    }

    /**
     * {@inheritdoc}
     */
    public function rollbackTransaction()
    {
        if (count($this->transactionStack)) {
            $name = array_pop($this->transactionStack);
            try {
                $this->pgQuery('ROLLBACK TO "' . $name . '"');
                $this->pgQuery('RELEASE SAVEPOINT "' . $name . '"');
            } catch (QueryException $ex) {
                throw new TransactionException(sprintf('Cannot rollback to savepoint %s', $name), null, $ex);
            }

            unset($this->savepointNames[$name]);
        } else {
            try {
                $this->pgQuery('ROLLBACK');
            } catch (QueryException $ex) {
                throw new TransactionException('Cannot cancel transaction', null, $ex);
            }
        }
    }

    /**
     * {@inheritdoc}
     */
    public function executeSqlCommand($sqlCommand)
    {
        return $this->pgQuery($sqlCommand);
    }

    /**
     * {@inheritdoc}
     */
    public function tableExists($tableName, $tableSchema = null)
    {
        $args = [$tableName];
        $sql = 'SELECT EXISTS (';
        $sql .= 'SELECT 1 FROM "information_schema"."tables" WHERE "table_name" = $1';
        if ($tableSchema) {
            $sql .= ' AND "table_schema" = $2';
            $args[] = $tableSchema;
        }
        $sql .= ')';

        $result = $this->pgQueryParams($sql, $args);
        $row = pg_fetch_row($result);

        return reset($row) === 't';
    }

    /**
     * {@inheritdoc}
     */
    public function recordExists($criteria, $tableName, $tableSchema = null)
    {
        $this->populateWhere($criteriaSql, $criteriaArguments, $criteria);
        $sql = 'SELECT EXISTS (SELECT 1 FROM '.$this->getTableLiteral($tableName, $tableSchema).' WHERE '.implode(' AND ', $criteriaSql).')';

        $result = $this->pgQueryParams($sql, $criteriaArguments);
        $row = pg_fetch_row($result);

        return reset($row) === 't';
    }

    /**
     * {@inheritdoc}
     */
    public function getRecordValues($fieldNames, $criteria, $tableName, $tableSchema = null)
    {
        $this->populateWhere($criteriaSql, $criteriaArguments, $criteria);
        $fieldsSql = [];
        foreach ($fieldNames as $fieldName) {
            $fieldsSql[] = '"'.$fieldName.'"';
        }
        $sql = 'SELECT '.implode(', ', $fieldsSql).' FROM '.$this->getTableLiteral($tableName, $tableSchema).' WHERE '.implode(' AND ', $criteriaSql);

        $result = $this->pgQueryParams($sql, $criteriaArguments);

        return pg_fetch_row($result);
    }

    /**
     * {@inheritdoc}
     */
    public function insertRecord($values, $tableName, $tableSchema = null)
    {
        $fieldNamesSql = [];
        $fieldValuesSql = [];
        $fieldsArguments = [];
        $n = 1;
        foreach ($values as $fieldName => $fieldValue) {
            $fieldNamesSql[] = '"'.$fieldName.'"';
            $fieldValuesSql[] = '$'.($n++);
            $fieldsArguments[] = $fieldValue;
        }
        $sql = 'INSERT INTO '.$this->getTableLiteral($tableName, $tableSchema).' ('.implode(', ', $fieldNamesSql).') VALUES ('.implode(', ', $fieldValuesSql).')';
        $this->pgQueryParams($sql, $fieldsArguments);
    }

    /**
     * {@inheritdoc}
     */
    public function updateRecord($values, $criteria, $tableName, $tableSchema = null)
    {
        $fieldsSql = [];
        $fieldsArguments = [];
        $n = 1;
        foreach ($values as $fieldName => $fieldValue) {
            $fieldsSql[] = '"'.$fieldName.'" = $'.($n++);
            $fieldsArguments[] = $fieldValue;
        }
        $this->populateWhere($criteriaSql, $criteriaArguments, $criteria, $n);
        $sql = 'UPDATE '.$this->getTableLiteral($tableName, $tableSchema).' SET '.implode(', ', $fieldsSql).' WHERE '.implode(' AND ', $criteriaSql);
        $this->pgQueryParams($sql, array_merge($fieldsArguments, $criteriaArguments));
    }

    protected function pgQuery($query)
    {
        return PgExceptionHelper::provideQueryException(function ($connection, $query) {
            return pg_query($connection, $query);
        }, [$this->dbh, $query]);
    }

    public function pgQueryParams($query, $params)
    {
        return PgExceptionHelper::provideQueryException(function ($connection, $query, $params) {
            return pg_query_params($connection, $query, $params);
        }, [$this->dbh, $query, $params]);
    }

    protected function getSavepointName()
    {
        do {
            $name = uniqid() . uniqid();
        } while (isset($this->savepointNames[$name]));

        return $name;
    }

    /**
     * @param array $sql
     * @param array $args
     * @param array $criteria
     * @param int $n
     */
    protected function populateWhere(&$sql, &$args, $criteria, $n = 1)
    {
        $sql = [];
        $args = [];
        foreach ($criteria as $field => $value) {
            $sql[] = '"'.$field.'" = $'.($n++);
            $args[] = $value;
        }
    }

    /**
     * Gets table literal (for using in queries).
     *
     * @return string
     */
    protected function getTableLiteral($tableName, $tableSchemaName = null)
    {
        $tableLiteral = '"'.$tableName.'"';

        if ($tableSchemaName) {
            $tableLiteral = '"'.$tableSchemaName.'".'.$tableLiteral;
        }

        return $tableLiteral;
    }
}
