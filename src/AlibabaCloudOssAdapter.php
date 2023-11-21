<?php
/*
 * 2022-2023 Tijs Driven Development
 *
 * NOTICE OF LICENSE
 *
 * This source file is subject to the Open Software License (OSL 3.0) that is available
 * through the world-wide-web at this URL: http://www.opensource.org/licenses/OSL-3.0
 * If you are unable to obtain it through the world-wide-web, please send an email
 * to magento@tijsdriven.dev so a copy can be sent immediately.
 *
 * @author Tijs van Raaij
 * @copyright 2022-2023 Tijs Driven Development
 * @license http://www.opensource.org/licenses/OSL-3.0 Open Software License (OSL 3.0)
 */

declare(strict_types=1);

namespace TijsDriven\Flysystem\AlibabaCloudOss;

use League\Flysystem\Config;
use League\Flysystem\DirectoryAttributes;
use League\Flysystem\FileAttributes;
use League\Flysystem\FilesystemAdapter;
use League\Flysystem\UnableToCopyFile;
use League\Flysystem\UnableToDeleteFile;
use League\Flysystem\UnableToMoveFile;
use League\Flysystem\UnableToReadFile;
use League\Flysystem\UnableToRetrieveMetadata;
use League\Flysystem\UnableToSetVisibility;
use League\Flysystem\UnableToWriteFile;
use OSS\Model\PrefixInfo;
use OSS\OssClient;
use Throwable;
use function array_merge_recursive;
use function file_put_contents;
use function fopen;
use function rtrim;
use function strtotime;
use function sys_get_temp_dir;
use function trim;
use function var_dump;

class AlibabaCloudOssAdapter implements FilesystemAdapter
{

    public function __construct(
        private OssClient $client,
        private string    $bucket,
        private array     $options = []
    )
    {

    }

    /**
     * @inheritDoc
     */
    public function fileExists(string $path): bool
    {
        try {
            return $this->client->doesObjectExist($this->bucket, $path, $this->options);
        } catch (Throwable $exception) {
            throw \League\Flysystem\UnableToCheckFileExistence::forLocation($path, $exception);
        }
    }

    /**
     * @inheritDoc
     */
    public function write(string $path, string $contents, Config $config): void
    {
        // @todo: see if there is a better way to do this
        $visibility = $config->get('visibility');
        if ($config->get('visibility') == \League\Flysystem\Visibility::PUBLIC) {
            $visibility = OssClient::OSS_ACL_TYPE_PUBLIC_READ_WRITE;
        } elseif ($config->get('visibility') == \League\Flysystem\Visibility::PRIVATE) {
            $visibility = OssClient::OSS_ACL_TYPE_PUBLIC_READ;
        }

        try {
            $options[OssClient::OSS_HEADERS] = [
                'x-oss-object-acl' => $visibility ?? 'default'
            ];

            $this->client->putObject($this->bucket, $path, $contents, array_merge_recursive($this->options, $options));
        } catch (Throwable $exception) {

            throw UnableToWriteFile::atLocation($path, '', $exception);
        }
    }

    /**
     * @inheritDoc
     */
    public function writeStream(string $path, $contents, Config $config): void
    {
        try {
            $this->client->uploadStream($this->bucket, $path, $contents, $this->options);
        } catch (Throwable $exception) {
            throw UnableToWriteFile::atLocation($path, '', $exception);
        }
    }

    /**
     * @inheritDoc
     */
    public function read(string $path): string
    {
        try {
            return $this->client->getObject($this->bucket, $path, $this->options);
        } catch (Throwable $exception) {
            throw UnableToReadFile::fromLocation($path, '', $exception);
        }
    }

    /**
     * @inheritDoc
     */
    public function readStream(string $path)
    {
        // @todo: see if there is a better way to do this
        try {
            $file = $this->client->getObject($this->bucket, $path, $this->options);
            if(!$file) {
                throw UnableToReadFile::fromLocation($path);
            }
            $tempName = tempnam(sys_get_temp_dir(), 'alibaba-oss-');
            file_put_contents($tempName, $file);

            return fopen($tempName, 'r');
        } catch (Throwable $exception) {
            throw UnableToReadFile::fromLocation($path, '', $exception);
        }
    }

    /**
     * @inheritDoc
     */
    public function delete(string $path): void
    {
        try {
            $this->client->deleteObject($this->bucket, $path, $this->options);
        } catch (Throwable $exception) {
            throw UnableToDeleteFile::atLocation($path, '', $exception);
        }
    }

    /**
     * @inheritDoc
     */
    public function deleteDirectory(string $path): void
    {
        try {
            $prefix = (empty($path) || str_ends_with($path, '/')) ? $path : $path . '/';
            $options = [
                OssClient::OSS_MARKER => null,
                // Specify the full path of the directory that you want to delete. The full path of the directory cannot contain the bucket name.
                OssClient::OSS_PREFIX => $prefix
            ];

            // have to delete all the objects in the directory first
            $truncated = false;
            while (!$truncated) {
                $result = $this->client->listObjects($this->bucket, array_merge_recursive($this->options, $options));
                $objects = [];
                if (count($result->getObjectList()) > 0) {
                    foreach ($result->getObjectList() as $object) {
                        $objects[] = $object->getKey();
                    }
                    $this->client->deleteObjects($this->bucket, $objects);
                }

                if ($result->getIsTruncated() === 'true') {
                    $options[OssClient::OSS_MARKER] = $result->getNextMarker();
                } else {
                    $truncated = true;
                }
            }

            $this->client->deleteObject($this->bucket, $path, array_merge_recursive($this->options, $options));
        } catch (Throwable $exception) {
            throw UnableToDeleteFile::atLocation($path, '', $exception);
        }
    }

    /**
     * @inheritDoc
     */
    public function createDirectory(string $path, Config $config): void
    {
        $this->client->createObjectDir($this->bucket, $path, $this->options);
    }

    /**
     * @inheritDoc
     */
    public function setVisibility(string $path, string $visibility): void
    {
        if ($visibility == \League\Flysystem\Visibility::PUBLIC) {
            $visibility = 'public-read';
        }

        try {
            $this->client->putObjectAcl($this->bucket, $path, $visibility, $this->options);
        } catch (Throwable $exception) {
            throw UnableToSetVisibility::atLocation($path, '', $exception);
        }
    }

    /**
     * @inheritDoc
     */
    public function visibility(string $path): FileAttributes
    {
        try {
            $result = $this->client->getObjectAcl($this->bucket, $path, $this->options);
        } catch (Throwable $exception) {
            throw UnableToRetrieveMetadata::visibility($path, '', $exception);
        }

        if ($result == OssClient::OSS_ACL_TYPE_PUBLIC_READ || $result == OssClient::OSS_ACL_TYPE_PUBLIC_READ_WRITE) {
            $result = \League\Flysystem\Visibility::PUBLIC;
        }

        return new FileAttributes($path, null, $result);
    }

    /**
     * @inheritDoc
     */
    public function mimeType(string $path): FileAttributes
    {
        try {
            $path = trim($path, '/');
            $result = $this->client->getObjectMeta($this->bucket, $path, $this->options);
        } catch (Throwable $exception) {
            throw UnableToRetrieveMetadata::mimeType($path, '', $exception);
        }

        if ($result['content-type'] == OssClient::DEFAULT_CONTENT_TYPE) {
            throw UnableToRetrieveMetadata::mimeType($path);
        }

        return new FileAttributes($path, null, null, null, $result['content-type']);
    }

    /**
     * @inheritDoc
     */
    public function lastModified(string $path): FileAttributes
    {
        try {
            $path = trim($path, '/');
            $result = $this->client->getObjectMeta($this->bucket, $path, $this->options);
        } catch (Throwable $exception) {
            throw UnableToRetrieveMetadata::lastModified($path, '', $exception);
        }

        return new FileAttributes($path, null, null, strtotime($result['last-modified']));
    }

    /**
     * @inheritDoc
     */
    public function fileSize(string $path): FileAttributes
    {

        try {
            $path = trim($path, '/');
            $result = $this->client->getObjectMeta($this->bucket, $path, $this->options);
        } catch (Throwable $exception) {
            throw UnableToRetrieveMetadata::fileSize($path, '', $exception);
        }
        return new FileAttributes($path, (int)$result['content-length']);
    }

    /**
     * @inheritDoc
     * @throws \OSS\Core\OssException
     */
    public function listContents(string $path, bool $deep): iterable
    {
        $options = [
            OssClient::OSS_PREFIX => (empty($path) || str_ends_with($path, '/')) ? $path : $path . '/',
            OssClient::OSS_DELIMITER => '/',
            OssClient::OSS_FETCH_OWNER => 'true',
        ];

        $listObjectInfo = $this->client->listObjectsV2($this->bucket, array_merge_recursive($this->options, $options));

        $objectList = $listObjectInfo->getObjectList();
        $prefixList = $listObjectInfo->getPrefixList();

        $keys = [];
        if ($deep === false) {
            $keys = $this->addFilesAndDirs($objectList, $keys, $prefixList);
        } else {
            // need to loop through nested directories like this as AlibabaCloud OSS does not seem to offer this
            while (!empty($prefixList)) {
                foreach ($prefixList as $item) {
                    $options[OssClient::OSS_PREFIX] = (empty($item->getPrefix()) || str_ends_with($item->getPrefix(), '/')) ? $item->getPrefix() : $item->getPrefix() . '/';

                    $listObjectInfo = $this->client->listObjectsV2($this->bucket, array_merge_recursive($this->options, $options));
                    $objectList = $listObjectInfo->getObjectList();
                    $prefixList = $listObjectInfo->getPrefixList();

                    $keys = $this->addFilesAndDirs($objectList, $keys, $prefixList);
                }
            }
        }

        foreach ($keys as $key) {
            yield $this->mapObjectMetaData($key);
        }
    }

    /**
     * @inheritDoc
     */
    public function move(string $source, string $destination, Config $config): void
    {
        try {
            $this->client->copyObject($this->bucket, $source, $this->bucket, $destination, $this->options);
            $this->client->deleteObject($this->bucket, $source, $this->options);
        } catch (Throwable $exception) {
            throw UnableToMoveFile::fromLocationTo($source, $destination, $exception);
        }
    }

    /**
     * @inheritDoc
     */
    public function copy(string $source, string $destination, Config $config): void
    {
        try {
            $this->client->copyObject($this->bucket, $source, $this->bucket, $destination, $this->options);
        } catch (Throwable $exception) {
            throw UnableToCopyFile::fromLocationTo($source, $destination, $exception);
        }
    }

    /**
     * @param array $objectList
     * @param array $keys
     * @param array $prefixList
     * @return array
     */
    private function addFilesAndDirs(array $objectList, array $keys, array $prefixList): array
    {
        if (!empty($objectList)) {
            foreach ($objectList as $objectInfo) {
                $keys[] = $objectInfo;
            }
        }
        if (!empty($prefixList)) {
            foreach ($prefixList as $item) {
                $keys[] = $item;
            }
        }
        return $keys;
    }

    private function mapObjectMetaData($key): FileAttributes|DirectoryAttributes
    {
        if ($key instanceof PrefixInfo) {
            return new DirectoryAttributes(rtrim($key->getPrefix(), '/'));
        }

        return new FileAttributes(
            $key->getKey(),
            $key->getSize(),
            null,
            strtotime($key->getLastModified()),
            $key->getType(),
            []
        );
    }
}
