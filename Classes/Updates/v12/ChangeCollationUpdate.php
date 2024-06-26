<?php


declare(strict_types=1);

/*
 * This file is part of the TYPO3 CMS project.
 *
 * It is free software; you can redistribute it and/or modify it under
 * the terms of the GNU General Public License, either version 2
 * of the License, or any later version.
 *
 * For the full copyright and license information, please read the
 * LICENSE.txt file that was distributed with this source code.
 *
 * The TYPO3 project - inspiring people to share!
 */

namespace TYPO3\CMS\v12\Install\Updates;

use Doctrine\DBAL\Schema\AbstractSchemaManager;
use Doctrine\DBAL\Schema\Table;
use TYPO3\CMS\Core\Database\Connection;
use TYPO3\CMS\Core\Database\ConnectionPool;
use TYPO3\CMS\Core\Utility\GeneralUtility;
use TYPO3\CMS\Install\Attribute\UpgradeWizard;
use TYPO3\CMS\Install\Updates\UpgradeWizardInterface;

/**
 */
#[UpgradeWizard('changeCollationUpdate')]
final class ChangeCollationUpdate implements UpgradeWizardInterface
{

    protected $charset;
    protected $collate;

    public function __construct()
    {

        $this->charset = $GLOBALS['TYPO3_CONF_VARS']['DB']['Connections']['Default']['tableoptions']['charset'] ?? 'utf8mb4';
        $this->collate = $GLOBALS['TYPO3_CONF_VARS']['DB']['Connections']['Default']['tableoptions']['collate'] ?? 'utf8mb4_general_ci';
    }


    /**
     * Returns the title for this update
     *
     * @return string
     */
    public function getTitle(): string
    {
        return 'Change database collation to match settings.php configuration';
    }

    /**
     * Returns the description for this update
     *
     * @return string
     */
    public function getDescription(): string
    {
        return 'This update will change the collation of all tables and fields to match the charset and collation settings in settings.php.';
    }


    public function updateNecessary(): bool
    {
        if (!isset($GLOBALS['TYPO3_CONF_VARS']['DB']['Connections']['Default']['tableoptions']['charset']) || !isset($GLOBALS['TYPO3_CONF_VARS']['DB']['Connections']['Default']['tableoptions']['collate'])) {
            return false;
        }

        $connection = GeneralUtility::makeInstance(ConnectionPool::class)->getConnectionByName('Default');
        $schemaManager = $connection->getSchemaManager();

        /** @var Table[] $tables */
        $tables = $schemaManager->listTables();

        foreach ($tables as $table) {
            $tableName = $table->getName();

            // Check table collation
            $tableCollation = $this->getTableCollation($connection, $schemaManager, $tableName);
            if ($tableCollation !== $this->collate) {
                return true;
            }

            // Check each column collation
            foreach ($table->getColumns() as $column) {

                if ($column->getType()->getName() === 'string' || $column->getType()->getName() === 'text') {
                    $columnCollation = $this->getColumnCollation($connection, $schemaManager, $tableName, $column->getName());
                    if ($columnCollation !== $this->collate) {
                        return true;
                    }
                }
            }
        }

        return false;
    }


    /**
     * Performs the update
     *
     * @return bool
     */
    public function executeUpdate(): bool
    {
        $connection = GeneralUtility::makeInstance(ConnectionPool::class)->getConnectionByName('Default');
        $schemaManager = $connection->getSchemaManager();

        /** @var Table[] $tables */
        $tables = $schemaManager->listTables();

        foreach ($tables as $table) {
            $tableName = $table->getName();

            // Change table collation
            $sql = sprintf(
                'ALTER TABLE %s CONVERT TO CHARACTER SET %s COLLATE %s;',
                $tableName,
                $this->charset,
                $this->collate
            );
            $connection->executeStatement($sql);

            // Change each column collation
            foreach ($table->getColumns() as $column) {
                if ($column->getType()->getName() === 'string' || $column->getType()->getName() === 'text') {
                    $columnName = $column->getName();
                    $columnType = $column->getType()->getSQLDeclaration($column->toArray(), $connection->getDatabasePlatform());

                    $sql = sprintf(
                        'ALTER TABLE %s CHANGE \'%s\' \'%s\' %s COLLATE %s;',
                        $tableName,
                        $columnName,
                        $columnName,
                        strtoupper($columnType),
                        $this->collate
                    );
                    $connection->executeStatement($sql);
                }
            }
        }

        return true;
    }

    public function getPrerequisites(): array
    {
        return [
        ];
    }


    /**
     * Get the collation of a table
     *
     * @param AbstractSchemaManager $schemaManager
     * @param string $tableName
     * @return string|null
     */
    protected function getTableCollation(Connection $connection, AbstractSchemaManager $schemaManager, string $tableName): ?string
    {
        $sql = sprintf('SHOW TABLE STATUS LIKE \'%s\'', $tableName);
        $result = $connection->fetchAssociative($sql);
        return $result['Collation'] ?? null;
    }

    /**
     * Get the collation of a column
     *
     * @param AbstractSchemaManager $schemaManager
     * @param string $tableName
     * @param string $columnName
     * @return string|null
     */
    protected function getColumnCollation(Connection $connection, AbstractSchemaManager $schemaManager, string $tableName, string $columnName): ?string
    {
        $sql = sprintf('SHOW FULL COLUMNS FROM %s LIKE \'%s\'', $tableName, $columnName);
        $result = $connection->fetchAssociative($sql);
        return $result['Collation'] ?? null;
    }


    /**
     * Get the SQL type of a column
     *
     * @param \Doctrine\DBAL\Schema\Column $column
     * @return string
     */
    protected function getColumnType(Connection $connection, \Doctrine\DBAL\Schema\Column $column): string
    {
        return $column->getType()->getSQLDeclaration($column->toArray(), $connection->getDatabasePlatform());
    }
}
