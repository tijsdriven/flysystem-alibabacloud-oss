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

namespace TijsDriven\Flysystem\AlibabaCloudOss\Test;

use AlibabaCloud\SDK\Sts\V20150401\Models\AssumeRoleRequest;
use AlibabaCloud\SDK\Sts\V20150401\Sts;
use AlibabaCloud\Tea\Utils\Utils\RuntimeOptions;
use League\Flysystem\AdapterTestUtilities\FilesystemAdapterTestCase;
use League\Flysystem\FilesystemAdapter;
use OSS\OssClient;
use TijsDriven\Flysystem\AlibabaCloudOss\AlibabaCloudOssAdapter;

class AlibabaCloudOssAdapterTest extends FilesystemAdapterTestCase
{

    protected static function createFilesystemAdapter(): FilesystemAdapter
    {
        try {
            $context = include __DIR__ . '/config/context.php';
        } catch (\Throwable $e) {
            throw new \RuntimeException('Missing config');
        }

        $config = new \Darabonba\OpenApi\Models\Config();
        $config->regionId = $context['regionId'];
        $config->accessKeyId = $context['accessKeyId'];
        $config->accessKeySecret = $context['accessKeySecret'];
        $config->endpoint = $context['stsEndpoint'];

        $sts = new Sts($config);

        $roleRequest = new AssumeRoleRequest([
            'roleArn' => $context['arn'],
            'roleSessionName' => $context['sessionName'],
            'durationSeconds' => $context['lifetime']
        ]);

        $runtimeOptions = new RuntimeOptions();

        $token = $sts->assumeRoleWithOptions($roleRequest, $runtimeOptions);

        $client = new OssClient(
            $token->body->credentials->accessKeyId,
            $token->body->credentials->accessKeySecret,
            $context['ossEndpoint'],
            false,
            $token->body->credentials->securityToken
        );

        return new AlibabaCloudOssAdapter($client, $context['bucket']);
    }
}
