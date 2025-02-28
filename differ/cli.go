/*
Copyright © 2023 Red Hat, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package differ

import (
	"flag"
	"fmt"
	"os"

	"github.com/RedHatInsights/ccx-notification-service/conf"
	"github.com/RedHatInsights/ccx-notification-service/types"
	"github.com/rs/zerolog/log"
)

// ShowVersion function displays version information.
func ShowVersion() {
	fmt.Println(versionMessage)
}

// ShowAuthors function displays information about authors.
func ShowAuthors() {
	fmt.Println(authorsMessage)
}

// ShowConfiguration function displays actual configuration.
func ShowConfiguration(config *conf.ConfigStruct) {
	brokerConfig := conf.GetKafkaBrokerConfiguration(config)
	log.Info().
		Bool("Enabled", brokerConfig.Enabled).
		Str("Address", brokerConfig.Address).
		Str("SecurityProtocol", brokerConfig.SecurityProtocol).
		Str("SaslMechanism", brokerConfig.SaslMechanism).
		Str("Topic", brokerConfig.Topic).
		Str("Timeout", brokerConfig.Timeout.String()).
		Int("Likelihood threshold", brokerConfig.LikelihoodThreshold).
		Int("Impact threshold", brokerConfig.ImpactThreshold).
		Int("Severity threshold", brokerConfig.SeverityThreshold).
		Int("Total risk threshold", brokerConfig.TotalRiskThreshold).
		Str("CoolDown", brokerConfig.Cooldown).
		Str("Event filter", brokerConfig.EventFilter).
		Bool("Filter by tags", brokerConfig.TagFilterEnabled).
		Strs("List of tags", brokerConfig.Tags).
		Msg("Broker configuration")

	serviceLogConfig := conf.GetServiceLogConfiguration(config)
	log.Info().
		Bool("Enabled", serviceLogConfig.Enabled).
		Str("ClientID", serviceLogConfig.ClientID).
		Str("Created by", serviceLogConfig.CreatedBy).
		Str("Username", serviceLogConfig.Username).
		Int("Likelihood threshold", brokerConfig.LikelihoodThreshold).
		Int("Impact threshold", brokerConfig.ImpactThreshold).
		Int("Severity threshold", brokerConfig.SeverityThreshold).
		Int("Total risk threshold", serviceLogConfig.TotalRiskThreshold).
		Str("CoolDown", serviceLogConfig.Cooldown).
		Str("Event filter", serviceLogConfig.EventFilter).
		Str("OCM URL", serviceLogConfig.URL).
		Bool("Filter by tags", serviceLogConfig.TagFilterEnabled).
		Strs("List of tags", serviceLogConfig.Tags).
		Msg("ServiceLog configuration")

	storageConfig := conf.GetStorageConfiguration(config)
	log.Info().
		Str("Driver", storageConfig.Driver).
		Str("DB Name", storageConfig.PGDBName).
		Str("Username", storageConfig.PGUsername). // password is omitted on purpose
		Str("Host", storageConfig.PGHost).
		Int("Port", storageConfig.PGPort).
		Bool("LogSQLQueries", storageConfig.LogSQLQueries).
		Str("Parameters", storageConfig.PGParams).
		Msg("Storage configuration")

	dependenciesConfig := conf.GetDependenciesConfiguration(config)
	log.Info().
		Str("Content server", dependenciesConfig.ContentServiceServer).
		Str("Content endpoint", dependenciesConfig.ContentServiceEndpoint).
		Str("Template renderer server", dependenciesConfig.TemplateRendererServer).
		Str("Template renderer endpoint", dependenciesConfig.TemplateRendererEndpoint).
		Msg("Dependencies configuration")

	loggingConfig := conf.GetLoggingConfiguration(config)
	log.Info().
		Str("Level", loggingConfig.LogLevel).
		Bool("Pretty colored debug logging", loggingConfig.Debug).
		Msg("Logging configuration")

	notificationConfig := conf.GetNotificationsConfiguration(config)
	log.Info().
		Str("Insights Advisor URL", notificationConfig.InsightsAdvisorURL).
		Str("Cluster details URI", notificationConfig.ClusterDetailsURI).
		Str("Rule details URI", notificationConfig.RuleDetailsURI).
		Msg("Notifications configuration")

	metricsConfig := conf.GetMetricsConfiguration(config)

	// Authentication token and metrics groups values are omitted on
	// purpose
	log.Info().
		Str("Namespace", metricsConfig.Namespace).
		Str("Subsystem", metricsConfig.Subsystem).
		Str("Push Gateway", metricsConfig.GatewayURL).
		Int("Retries", metricsConfig.Retries).
		Str("Retry after", metricsConfig.RetryAfter.String()).
		Msg("Metrics configuration")

	processingConfig := conf.GetProcessingConfiguration(config)
	log.Info().
		Bool("Filter allowed clusters", processingConfig.FilterAllowedClusters).
		Strs("List of allowed clusters", processingConfig.AllowedClusters).
		Bool("Filter blocked clusters", processingConfig.FilterBlockedClusters).
		Strs("List of blocked clusters", processingConfig.BlockedClusters).
		Msg("Processing configuration")
}

func deleteOperationSpecified(cliFlags types.CliFlags) bool {
	return cliFlags.PrintNewReportsForCleanup ||
		cliFlags.PerformNewReportsCleanup ||
		cliFlags.PrintOldReportsForCleanup ||
		cliFlags.PerformOldReportsCleanup
}

// SetupCliFlags defines and parses all command line options
func setupCliFlags() types.CliFlags {
	var cliFlags types.CliFlags
	flag.BoolVar(&cliFlags.InstantReports, "instant-reports", false, "create instant reports")
	flag.BoolVar(&cliFlags.ShowVersion, "show-version", false, "show version and exit")
	flag.BoolVar(&cliFlags.ShowAuthors, "show-authors", false, "show authors and exit")
	flag.BoolVar(&cliFlags.ShowConfiguration, "show-configuration", false, "show configuration and exit")
	flag.BoolVar(&cliFlags.PrintNewReportsForCleanup, "print-new-reports-for-cleanup", false, "print new reports to be cleaned up")
	flag.BoolVar(&cliFlags.PerformNewReportsCleanup, "new-reports-cleanup", false, "perform new reports clean up")
	flag.BoolVar(&cliFlags.PrintOldReportsForCleanup, "print-old-reports-for-cleanup", false, "print old reports to be cleaned up")
	flag.BoolVar(&cliFlags.PerformOldReportsCleanup, "old-reports-cleanup", false, "perform old reports clean up")
	flag.BoolVar(&cliFlags.CleanupOnStartup, "cleanup-on-startup", false, "perform database clean up on startup")
	flag.BoolVar(&cliFlags.Verbose, "verbose", false, "verbose logs")
	flag.StringVar(&cliFlags.MaxAge, "max-age", "", "max age for displaying/cleaning old records")
	flag.Parse()
	return cliFlags
}

// checkArgs function handles command line options passed to the process
func checkArgs(args *types.CliFlags) {
	switch {
	case args.ShowVersion:
		ShowVersion()
		os.Exit(ExitStatusOK)
	case args.ShowAuthors:
		ShowAuthors()
		os.Exit(ExitStatusOK)
	case args.ShowConfiguration:
		// config not loaded yet, just skip the rest of function for
		// now
		return
	case args.PrintNewReportsForCleanup,
		args.PerformNewReportsCleanup,
		args.PrintOldReportsForCleanup,
		args.PerformOldReportsCleanup:
		// DB only operations, no need for additional args
		return
	default:
	}

	// check if report type is specified on command line
	// TODO: Do we still want this argument?
	if !args.InstantReports {
		log.Error().Msg("Type of report needs to be specified on command line")
		os.Exit(ExitStatusConfiguration)
	}
	notificationType = types.InstantNotif
}
