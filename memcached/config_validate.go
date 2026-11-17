package memcached

import (
	"crypto/tls"
	"fmt"
	"time"
)

// ApplyCacheLatencyTimeouts sets Dial/Read/Write timeouts to 100ms for
// fail-fast cache paths. Does not change package defaults unless called;
// TimeoutNone is cleared to 100ms.
func (cfg *Config) ApplyCacheLatencyTimeouts() {
	if cfg == nil {
		return
	}
	const d = 100 * time.Millisecond
	cfg.Transport.DialTimeout = d
	cfg.Transport.ReadTimeout = d
	cfg.Transport.WriteTimeout = d
}

// DefaultFailover returns the former multi-node policy: two failures, 1s retry, 30s dead, eject on.
func DefaultFailover() Failover {
	return Failover{
		FailureLimit: 2,
		RetryAfter:   time.Second,
		DeadAfter:    30 * time.Second,
		Eject:        true,
	}
}

// production pool lifetimes used by DefaultProduction / ApplyProductionDefaults.
const (
	_productionMaxLifetime    = 30 * time.Minute
	_productionMaxIdleTime    = 5 * time.Minute
	_productionIdleHealthPing = 30 * time.Second
)

// DefaultProduction returns multi-node production pool/failover defaults with
// empty Servers. Callers set Servers (or use NewProductionFromAddrs). Does not
// change Transport timeouts; combine with ApplyCacheLatencyTimeouts when needed.
func DefaultProduction() Config {
	return Config{
		Failover: DefaultFailover(),
		Pool: Pool{
			MaxLifetime:    _productionMaxLifetime,
			MaxIdleTime:    _productionMaxIdleTime,
			IdleHealthPing: _productionIdleHealthPing,
		},
	}
}

// ApplyProductionDefaults sets Failover, MaxLifetime, MaxIdleTime, and
// IdleHealthPing to DefaultProduction values. Other Config fields are unchanged.
// NewFromAddrs stays bare; call this (or NewProductionFromAddrs) explicitly.
func (cfg *Config) ApplyProductionDefaults() {
	if cfg == nil {
		return
	}
	cfg.Failover = DefaultFailover()
	cfg.Pool.MaxLifetime = _productionMaxLifetime
	cfg.Pool.MaxIdleTime = _productionMaxIdleTime
	cfg.Pool.IdleHealthPing = _productionIdleHealthPing
}

func validateConfigPool(cfg Config) error {
	if cfg.Pool.MaxOpen < 0 {
		return fmt.Errorf("Pool.MaxOpen must be >= 0: %w", ErrInvalidArgument)
	}
	if cfg.Pool.MinIdle < 0 {
		return fmt.Errorf("Pool.MinIdle must be >= 0: %w", ErrInvalidArgument)
	}
	maxOpen := cfg.Pool.MaxOpen
	if maxOpen == 0 {
		maxOpen = 100
	}
	if cfg.Pool.MinIdle > maxOpen {
		return fmt.Errorf("Pool.MinIdle exceeds MaxOpen: %w", ErrInvalidArgument)
	}
	if cfg.Pool.MinIdle > 0 && cfg.Pool.MaxIdle < 0 {
		return fmt.Errorf("Pool.MinIdle requires idle connections: %w", ErrInvalidArgument)
	}
	if cfg.Pool.MaxIdle > 0 && cfg.Pool.MinIdle > cfg.Pool.MaxIdle {
		return fmt.Errorf("Pool.MinIdle exceeds MaxIdle: %w", ErrInvalidArgument)
	}
	return nil
}

func validateConfigCluster(cfg Config) error {
	if cfg.Cluster.ResolverRefresh > 0 && !cfg.AllowSetServers {
		return fmt.Errorf("ResolverRefresh requires AllowSetServers: %w", ErrSetServersNotAllowed)
	}
	if err := validNamespace(cfg.Cluster.Namespace); err != nil {
		return err
	}
	if cfg.Cluster.FanoutWorkers < 0 {
		return fmt.Errorf("Cluster.FanoutWorkers must be >= 0: %w", ErrInvalidArgument)
	}
	if cfg.Cluster.MaxPipelineOps < 0 {
		return fmt.Errorf("Cluster.MaxPipelineOps must be >= 0: %w", ErrInvalidArgument)
	}
	if !cfg.Cluster.MemorystoreDiscovery {
		return nil
	}
	if cfg.Cluster.ResolverRefresh <= 0 {
		return fmt.Errorf("MemorystoreDiscovery requires ResolverRefresh: %w", ErrInvalidArgument)
	}
	if cfg.Cluster.ResolverRefresh < time.Second {
		return fmt.Errorf("MemorystoreDiscovery ResolverRefresh must be >= 1s: %w", ErrInvalidArgument)
	}
	if !cfg.AllowSetServers {
		return fmt.Errorf("MemorystoreDiscovery requires AllowSetServers: %w", ErrSetServersNotAllowed)
	}
	if serversAreUDP(cfg.Servers) {
		return fmt.Errorf("MemorystoreDiscovery is not supported on UDP: %w", ErrNotSupported)
	}
	return nil
}

func validateConfigResolveSpec(cfg Config) error {
	if cfg.Cluster.ResolveSpec == "" {
		return nil
	}
	specServers, err := parseServerSpec(cfg.Cluster.ResolveSpec)
	if err != nil {
		return err
	}
	if err := validateTransportServers(specServers, cfg.Transport.TLS); err != nil {
		return err
	}
	if serversAreUDP(cfg.Servers) != serversAreUDP(specServers) {
		return fmt.Errorf("ResolveSpec transport differs from Servers: %w", ErrMixedTransport)
	}
	if cfg.Auth == nil {
		return nil
	}
	return validateAuthAgainstServers(specServers, cfg.Auth.AllowPlaintext, cfg.Transport.TLS != nil)
}

func validateConfigTransport(cfg Config) error {
	if err := validateTransportServers(cfg.Servers, cfg.Transport.TLS); err != nil {
		return err
	}
	if cfg.Transport.RequireTLS && cfg.Transport.TLS == nil {
		return fmt.Errorf("RequireTLS requires Transport.TLS: %w", ErrInvalidArgument)
	}
	if cfg.Transport.TLS != nil &&
		cfg.Transport.TLS.MinVersion != 0 &&
		cfg.Transport.TLS.MinVersion < tls.VersionTLS12 {
		return fmt.Errorf("TLS.MinVersion must be TLS 1.2 or newer: %w", ErrInvalidArgument)
	}
	if cfg.Transport.TLS != nil && cfg.Transport.TLS.InsecureSkipVerify && !cfg.Transport.AllowInsecureTLS {
		return ErrInsecureTLS
	}
	return nil
}

func validateConfigAuth(cfg Config) error {
	if cfg.Auth == nil {
		return nil
	}
	if cfg.Auth.User == "" {
		return fmt.Errorf("auth user empty: %w", ErrInvalidArgument)
	}
	if err := validAuthSecret(cfg.Auth.User); err != nil {
		return err
	}
	if err := validAuthSecret(cfg.Auth.Pass); err != nil {
		return err
	}
	return validateAuthAgainstServers(cfg.Servers, cfg.Auth.AllowPlaintext, cfg.Transport.TLS != nil)
}

// validateConfig checks Config invariants before New builds a Client.
func validateConfig(cfg Config) error {
	if len(cfg.Servers) == 0 {
		return fmt.Errorf("empty address: %w", ErrInvalidAddress)
	}
	if err := validateConfigPool(cfg); err != nil {
		return err
	}
	if err := validateConfigCluster(cfg); err != nil {
		return err
	}
	if err := validateConfigTransport(cfg); err != nil {
		return err
	}
	if err := validateConfigResolveSpec(cfg); err != nil {
		return err
	}
	return validateConfigAuth(cfg)
}
