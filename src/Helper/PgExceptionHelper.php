<?php

namespace IKTO\PgSqlMigrationDirectories\Helper;

use IKTO\PgSqlMigrationDirectories\Exception\QueryException;

class PgExceptionHelper
{
    public static function provideQueryException($callback, $arguments)
    {
        if (!is_callable($callback)) {
            throw new \InvalidArgumentException('Wrong callable provided to ' . __METHOD__);
        }

        set_error_handler(function ($errno, $errstr) {
            throw new QueryException($errstr);
        });
        $result = call_user_func_array($callback, $arguments);
        restore_error_handler();

        return $result;
    }
}
