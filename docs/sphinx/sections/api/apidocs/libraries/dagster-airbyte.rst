Airbyte (dagster-airbyte)
---------------------------

This library provides a Dagster integration with `Airbyte <https://www.airbyte.com/>`_.

For more information on getting started, see the `Airbyte integration guide </integrations/airbyte>`_.

.. currentmodule:: dagster_airbyte

Ops
===

.. autoconfigurable:: airbyte_sync_op


Resources
=========

.. autoconfigurable:: airbyte_resource
    :annotation: ResourceDefinition

.. autoclass:: AirbyteResource
    :members:

Assets
======

.. autofunction:: load_assets_from_airbyte_instance

.. autofunction:: load_assets_from_airbyte_project

.. autofunction:: build_airbyte_assets


Managed Config
==============

.. autofunction:: load_assets_from_connections


.. autoclass:: AirbyteSource
.. autoclass:: AirbyteDestination
.. autoclass:: AirbyteConnection


Managed Config Generated Sources
================================

.. currentmodule:: dagster_airbyte.managed.generated.sources

.. autoclass:: StravaSource
    :members:

.. autoclass:: AppsflyerSource
    :members:

.. autoclass:: GoogleWorkspaceAdminReportsSource
    :members:

.. autoclass:: CartSource
    :members:

.. autoclass:: LinkedinAdsSource
    :members:

.. autoclass:: MongodbSource
    :members:

.. autoclass:: TimelySource
    :members:

.. autoclass:: StockTickerApiTutorialSource
    :members:

.. autoclass:: WrikeSource
    :members:

.. autoclass:: CommercetoolsSource
    :members:

.. autoclass:: GutendexSource
    :members:

.. autoclass:: IterableSource
    :members:

.. autoclass:: QuickbooksSingerSource
    :members:

.. autoclass:: BigcommerceSource
    :members:

.. autoclass:: ShopifySource
    :members:

.. autoclass:: AppstoreSingerSource
    :members:

.. autoclass:: GreenhouseSource
    :members:

.. autoclass:: ZoomSingerSource
    :members:

.. autoclass:: TiktokMarketingSource
    :members:

.. autoclass:: ZendeskChatSource
    :members:

.. autoclass:: AwsCloudtrailSource
    :members:

.. autoclass:: OktaSource
    :members:

.. autoclass:: InsightlySource
    :members:

.. autoclass:: LinkedinPagesSource
    :members:

.. autoclass:: PersistiqSource
    :members:

.. autoclass:: FreshcallerSource
    :members:

.. autoclass:: AppfollowSource
    :members:

.. autoclass:: FacebookPagesSource
    :members:

.. autoclass:: JiraSource
    :members:

.. autoclass:: GoogleSheetsSource
    :members:

.. autoclass:: DockerhubSource
    :members:

.. autoclass:: UsCensusSource
    :members:

.. autoclass:: KustomerSingerSource
    :members:

.. autoclass:: AzureTableSource
    :members:

.. autoclass:: ScaffoldJavaJdbcSource
    :members:

.. autoclass:: TidbSource
    :members:

.. autoclass:: QualarooSource
    :members:

.. autoclass:: YahooFinancePriceSource
    :members:

.. autoclass:: GoogleAnalyticsV4Source
    :members:

.. autoclass:: JdbcSource
    :members:

.. autoclass:: FakerSource
    :members:

.. autoclass:: TplcentralSource
    :members:

.. autoclass:: ClickhouseSource
    :members:

.. autoclass:: FreshserviceSource
    :members:

.. autoclass:: ZenloopSource
    :members:

.. autoclass:: OracleSource
    :members:

.. autoclass:: KlaviyoSource
    :members:

.. autoclass:: GoogleDirectorySource
    :members:

.. autoclass:: InstagramSource
    :members:

.. autoclass:: ShortioSource
    :members:

.. autoclass:: SquareSource
    :members:

.. autoclass:: DelightedSource
    :members:

.. autoclass:: AmazonSqsSource
    :members:

.. autoclass:: YoutubeAnalyticsSource
    :members:

.. autoclass:: ScaffoldSourcePythonSource
    :members:

.. autoclass:: LookerSource
    :members:

.. autoclass:: GitlabSource
    :members:

.. autoclass:: ExchangeRatesSource
    :members:

.. autoclass:: AmazonAdsSource
    :members:

.. autoclass:: MixpanelSource
    :members:

.. autoclass:: OrbitSource
    :members:

.. autoclass:: AmazonSellerPartnerSource
    :members:

.. autoclass:: CourierSource
    :members:

.. autoclass:: CloseComSource
    :members:

.. autoclass:: BingAdsSource
    :members:

.. autoclass:: PrimetricSource
    :members:

.. autoclass:: PivotalTrackerSource
    :members:

.. autoclass:: ElasticsearchSource
    :members:

.. autoclass:: BigquerySource
    :members:

.. autoclass:: WoocommerceSource
    :members:

.. autoclass:: SearchMetricsSource
    :members:

.. autoclass:: TypeformSource
    :members:

.. autoclass:: WebflowSource
    :members:

.. autoclass:: FireboltSource
    :members:

.. autoclass:: FaunaSource
    :members:

.. autoclass:: IntercomSource
    :members:

.. autoclass:: FreshsalesSource
    :members:

.. autoclass:: AdjustSource
    :members:

.. autoclass:: BambooHrSource
    :members:

.. autoclass:: GoogleAdsSource
    :members:

.. autoclass:: HellobatonSource
    :members:

.. autoclass:: SendgridSource
    :members:

.. autoclass:: MondaySource
    :members:

.. autoclass:: DixaSource
    :members:

.. autoclass:: SalesforceSource
    :members:

.. autoclass:: PipedriveSource
    :members:

.. autoclass:: FileSource
    :members:

.. autoclass:: GlassfrogSource
    :members:

.. autoclass:: ChartmogulSource
    :members:

.. autoclass:: OrbSource
    :members:

.. autoclass:: CockroachdbSource
    :members:

.. autoclass:: ConfluenceSource
    :members:

.. autoclass:: PlaidSource
    :members:

.. autoclass:: SnapchatMarketingSource
    :members:

.. autoclass:: MicrosoftTeamsSource
    :members:

.. autoclass:: LeverHiringSource
    :members:

.. autoclass:: TwilioSource
    :members:

.. autoclass:: StripeSource
    :members:

.. autoclass:: Db2Source
    :members:

.. autoclass:: SlackSource
    :members:

.. autoclass:: RechargeSource
    :members:

.. autoclass:: OpenweatherSource
    :members:

.. autoclass:: RetentlySource
    :members:

.. autoclass:: ScaffoldSourceHttpSource
    :members:

.. autoclass:: YandexMetricaSource
    :members:

.. autoclass:: TalkdeskExploreSource
    :members:

.. autoclass:: ChargifySource
    :members:

.. autoclass:: RkiCovidSource
    :members:

.. autoclass:: PostgresSource
    :members:

.. autoclass:: TrelloSource
    :members:

.. autoclass:: PrestashopSource
    :members:

.. autoclass:: PaystackSource
    :members:

.. autoclass:: S3Source
    :members:

.. autoclass:: SnowflakeSource
    :members:

.. autoclass:: AmplitudeSource
    :members:

.. autoclass:: PosthogSource
    :members:

.. autoclass:: PaypalTransactionSource
    :members:

.. autoclass:: MssqlSource
    :members:

.. autoclass:: ZohoCrmSource
    :members:

.. autoclass:: RedshiftSource
    :members:

.. autoclass:: AsanaSource
    :members:

.. autoclass:: SmartsheetsSource
    :members:

.. autoclass:: MailchimpSource
    :members:

.. autoclass:: SentrySource
    :members:

.. autoclass:: MailgunSource
    :members:

.. autoclass:: OnesignalSource
    :members:

.. autoclass:: PythonHttpTutorialSource
    :members:

.. autoclass:: AirtableSource
    :members:

.. autoclass:: MongodbV2Source
    :members:

.. autoclass:: FileSecureSource
    :members:

.. autoclass:: ZendeskSupportSource
    :members:

.. autoclass:: TempoSource
    :members:

.. autoclass:: BraintreeSource
    :members:

.. autoclass:: SalesloftSource
    :members:

.. autoclass:: LinnworksSource
    :members:

.. autoclass:: ChargebeeSource
    :members:

.. autoclass:: GoogleAnalyticsDataApiSource
    :members:

.. autoclass:: OutreachSource
    :members:

.. autoclass:: LemlistSource
    :members:

.. autoclass:: ApifyDatasetSource
    :members:

.. autoclass:: RecurlySource
    :members:

.. autoclass:: ZendeskTalkSource
    :members:

.. autoclass:: SftpSource
    :members:

.. autoclass:: WhiskyHunterSource
    :members:

.. autoclass:: FreshdeskSource
    :members:

.. autoclass:: GocardlessSource
    :members:

.. autoclass:: ZuoraSource
    :members:

.. autoclass:: MarketoSource
    :members:

.. autoclass:: DriftSource
    :members:

.. autoclass:: PokeapiSource
    :members:

.. autoclass:: NetsuiteSource
    :members:

.. autoclass:: HubplannerSource
    :members:

.. autoclass:: Dv360Source
    :members:

.. autoclass:: NotionSource
    :members:

.. autoclass:: ZendeskSunshineSource
    :members:

.. autoclass:: PinterestSource
    :members:

.. autoclass:: MetabaseSource
    :members:

.. autoclass:: HubspotSource
    :members:

.. autoclass:: HarvestSource
    :members:

.. autoclass:: GithubSource
    :members:

.. autoclass:: E2eTestSource
    :members:

.. autoclass:: MysqlSource
    :members:

.. autoclass:: MyHoursSource
    :members:

.. autoclass:: KyribaSource
    :members:

.. autoclass:: GoogleSearchConsoleSource
    :members:

.. autoclass:: FacebookMarketingSource
    :members:

.. autoclass:: SurveymonkeySource
    :members:

.. autoclass:: PardotSource
    :members:

.. autoclass:: FlexportSource
    :members:

.. autoclass:: ZenefitsSource
    :members:

.. autoclass:: KafkaSource
    :members:


Managed Config Generated Destinations
=====================================

.. currentmodule:: dagster_airbyte.managed.generated.destinations


.. autoclass:: DynamodbDestination
    :members:

.. autoclass:: BigqueryDestination
    :members:

.. autoclass:: RabbitmqDestination
    :members:

.. autoclass:: KvdbDestination
    :members:

.. autoclass:: ClickhouseDestination
    :members:

.. autoclass:: AmazonSqsDestination
    :members:

.. autoclass:: MariadbColumnstoreDestination
    :members:

.. autoclass:: KinesisDestination
    :members:

.. autoclass:: AzureBlobStorageDestination
    :members:

.. autoclass:: KafkaDestination
    :members:

.. autoclass:: ElasticsearchDestination
    :members:

.. autoclass:: MysqlDestination
    :members:

.. autoclass:: SftpJsonDestination
    :members:

.. autoclass:: GcsDestination
    :members:

.. autoclass:: CassandraDestination
    :members:

.. autoclass:: FireboltDestination
    :members:

.. autoclass:: GoogleSheetsDestination
    :members:

.. autoclass:: DatabricksDestination
    :members:

.. autoclass:: BigqueryDenormalizedDestination
    :members:

.. autoclass:: SqliteDestination
    :members:

.. autoclass:: MongodbDestination
    :members:

.. autoclass:: RocksetDestination
    :members:

.. autoclass:: OracleDestination
    :members:

.. autoclass:: CsvDestination
    :members:

.. autoclass:: S3Destination
    :members:

.. autoclass:: AwsDatalakeDestination
    :members:

.. autoclass:: MssqlDestination
    :members:

.. autoclass:: PubsubDestination
    :members:

.. autoclass:: R2Destination
    :members:

.. autoclass:: JdbcDestination
    :members:

.. autoclass:: KeenDestination
    :members:

.. autoclass:: TidbDestination
    :members:

.. autoclass:: FirestoreDestination
    :members:

.. autoclass:: ScyllaDestination
    :members:

.. autoclass:: RedisDestination
    :members:

.. autoclass:: MqttDestination
    :members:

.. autoclass:: RedshiftDestination
    :members:

.. autoclass:: PulsarDestination
    :members:

.. autoclass:: SnowflakeDestination
    :members:

.. autoclass:: PostgresDestination
    :members:

.. autoclass:: ScaffoldDestinationPythonDestination
    :members:

.. autoclass:: LocalJsonDestination
    :members:

.. autoclass:: MeilisearchDestination
    :members: