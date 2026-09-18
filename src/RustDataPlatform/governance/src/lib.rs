use anyhow::{Context, Result};
use async_trait::async_trait;
use replenishment_agent::{
    ExecutionAttempt, PolicyDecision, ReorderDecision, ReorderProposal, ReorderSimulation,
};
use tokio_postgres::{Client, Config, NoTls};

#[async_trait]
pub trait GovernanceStore: Send + Sync {
    async fn initialize(&self) -> Result<()>;
    async fn record_evaluation(&self, decision: &ReorderDecision) -> Result<()>;
    async fn record_simulation(&self, simulation: &ReorderSimulation) -> Result<()>;
    async fn record_proposal(&self, proposal: &ReorderProposal) -> Result<()>;
    async fn record_policy_decision(&self, decision: &PolicyDecision) -> Result<()>;
    async fn record_execution_attempt(&self, attempt: &ExecutionAttempt) -> Result<()>;
}

pub struct PostgresGovernanceStore {
    client: Client,
}

impl PostgresGovernanceStore {
    pub async fn connect(connection_string: &str) -> Result<Self> {
        let (client, connection) = connect_postgres(connection_string).await?;
        tokio::spawn(async move {
            if let Err(error) = connection.await {
                eprintln!("governance PostgreSQL connection stopped: {error}");
            }
        });
        Ok(Self { client })
    }
}

async fn connect_postgres(
    connection_string: &str,
) -> Result<(
    Client,
    impl std::future::Future<Output = std::result::Result<(), tokio_postgres::Error>> + Send + 'static,
)> {
    if connection_string.contains(';') {
        parse_npgsql_connection_string(connection_string)?
            .connect(NoTls)
            .await
            .context("governance PostgreSQL connection failed")
    } else {
        tokio_postgres::connect(connection_string, NoTls)
            .await
            .context("governance PostgreSQL connection failed")
    }
}

fn parse_npgsql_connection_string(connection_string: &str) -> Result<Config> {
    let mut config = Config::new();
    for part in connection_string.split(';') {
        if part.trim().is_empty() {
            continue;
        }
        let (key, value) = part
            .split_once('=')
            .with_context(|| format!("invalid governance connection string segment: {part}"))?;
        let key = key.trim().to_ascii_lowercase().replace(' ', "");
        let value = value.trim();
        match key.as_str() {
            "host" | "server" => {
                config.host(value);
            }
            "port" => {
                config.port(value.parse().context("PostgreSQL port must be numeric")?);
            }
            "database" | "dbname" => {
                config.dbname(value);
            }
            "username" | "userid" | "user" => {
                config.user(value);
            }
            "password" => {
                config.password(value);
            }
            "sslmode" | "pooling" | "includeerror detail" | "includeerrordetail" => {}
            _ => {}
        }
    }
    Ok(config)
}

#[async_trait]
impl GovernanceStore for PostgresGovernanceStore {
    async fn initialize(&self) -> Result<()> {
        self.client
            .batch_execute(governance_schema_sql())
            .await
            .context("governance schema initialization failed")
    }

    async fn record_evaluation(&self, decision: &ReorderDecision) -> Result<()> {
        self.client
            .execute(
                "INSERT INTO governance.agent_evaluations
                 (decision_id, sku_id, location_code, created_at, risk_level, recommended_quantity,
                  model_version, horizon_demand_units, reason_codes)
                 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
                 ON CONFLICT (decision_id) DO NOTHING",
                &[
                    &decision.decision_id,
                    &decision.sku_id,
                    &decision.location_code,
                    &decision.created_at,
                    &format!("{:?}", decision.risk_level).to_ascii_uppercase(),
                    &decision.recommended_quantity,
                    &decision.model_version,
                    &decision.horizon_demand_units,
                    &decision.reason_codes,
                ],
            )
            .await
            .context("recording governance evaluation failed")?;
        Ok(())
    }

    async fn record_simulation(&self, simulation: &ReorderSimulation) -> Result<()> {
        self.client
            .execute(
                "INSERT INTO governance.reorder_simulations
                 (simulation_id, decision_id, sku_id, location_code, evaluated_at,
                  selected_quantity, alternatives_json)
                 VALUES ($1,$2,$3,$4,$5,$6,$7::jsonb)
                 ON CONFLICT (simulation_id) DO NOTHING",
                &[
                    &simulation.simulation_id,
                    &simulation.decision_id,
                    &simulation.sku_id,
                    &simulation.location_code,
                    &simulation.evaluated_at,
                    &simulation.selected_quantity,
                    &serde_json::to_value(&simulation.alternatives)?,
                ],
            )
            .await
            .context("recording governance simulation failed")?;
        Ok(())
    }

    async fn record_proposal(&self, proposal: &ReorderProposal) -> Result<()> {
        self.client
            .execute(
                "INSERT INTO governance.reorder_proposals
                 (proposal_id, decision_id, sku_id, location_code, quantity, status,
                  created_at, updated_at, correlation_id, reasoning_json)
                 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10::jsonb)
                 ON CONFLICT (proposal_id) DO UPDATE
                 SET status = EXCLUDED.status, updated_at = EXCLUDED.updated_at,
                     reasoning_json = EXCLUDED.reasoning_json",
                &[
                    &proposal.proposal_id,
                    &proposal.decision_id,
                    &proposal.sku_id,
                    &proposal.location_code,
                    &proposal.quantity,
                    &format!("{:?}", proposal.status).to_ascii_uppercase(),
                    &proposal.created_at,
                    &proposal.updated_at,
                    &proposal.correlation_id,
                    &serde_json::to_value(&proposal.reasoning)?,
                ],
            )
            .await
            .context("recording governance proposal failed")?;
        Ok(())
    }

    async fn record_policy_decision(&self, decision: &PolicyDecision) -> Result<()> {
        self.client
            .execute(
                "INSERT INTO governance.policy_decisions
                 (policy_decision_id, proposal_id, outcome, reason_codes, policy_version, decided_at)
                 VALUES ($1,$2,$3,$4,$5,$6)
                 ON CONFLICT (policy_decision_id) DO NOTHING",
                &[
                    &decision.policy_decision_id,
                    &decision.proposal_id,
                    &format!("{:?}", decision.outcome).to_ascii_uppercase(),
                    &decision.reason_codes,
                    &decision.policy_version,
                    &decision.decided_at,
                ],
            )
            .await
            .context("recording governance policy decision failed")?;
        Ok(())
    }

    async fn record_execution_attempt(&self, attempt: &ExecutionAttempt) -> Result<()> {
        self.client
            .execute(
                "INSERT INTO governance.execution_attempts
                 (attempt_id, proposal_id, operation_id, status, message, attempted_at)
                 VALUES ($1,$2,$3,$4,$5,$6)
                 ON CONFLICT (attempt_id) DO NOTHING",
                &[
                    &attempt.attempt_id,
                    &attempt.proposal_id,
                    &attempt.operation_id,
                    &format!("{:?}", attempt.status).to_ascii_uppercase(),
                    &attempt.message,
                    &attempt.attempted_at,
                ],
            )
            .await
            .context("recording governance execution attempt failed")?;
        Ok(())
    }
}

pub fn governance_schema_sql() -> &'static str {
    r#"
CREATE SCHEMA IF NOT EXISTS governance;

CREATE TABLE IF NOT EXISTS governance.agent_evaluations (
    decision_id uuid PRIMARY KEY,
    sku_id integer NOT NULL,
    location_code text NOT NULL,
    created_at timestamptz NOT NULL,
    risk_level text NOT NULL,
    recommended_quantity integer NOT NULL CHECK (recommended_quantity >= 0),
    model_version text NOT NULL,
    horizon_demand_units double precision NOT NULL CHECK (horizon_demand_units >= 0),
    reason_codes text[] NOT NULL
);

CREATE TABLE IF NOT EXISTS governance.reorder_simulations (
    simulation_id uuid PRIMARY KEY,
    decision_id uuid NOT NULL REFERENCES governance.agent_evaluations(decision_id),
    sku_id integer NOT NULL,
    location_code text NOT NULL,
    evaluated_at timestamptz NOT NULL,
    selected_quantity integer NOT NULL CHECK (selected_quantity >= 0),
    alternatives_json jsonb NOT NULL
);

CREATE TABLE IF NOT EXISTS governance.reorder_proposals (
    proposal_id uuid PRIMARY KEY,
    decision_id uuid NOT NULL REFERENCES governance.agent_evaluations(decision_id),
    sku_id integer NOT NULL,
    location_code text NOT NULL,
    quantity integer NOT NULL CHECK (quantity >= 0),
    status text NOT NULL,
    created_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL,
    correlation_id uuid NOT NULL,
    reasoning_json jsonb NOT NULL
);

CREATE TABLE IF NOT EXISTS governance.policy_decisions (
    policy_decision_id uuid PRIMARY KEY,
    proposal_id uuid NOT NULL REFERENCES governance.reorder_proposals(proposal_id),
    outcome text NOT NULL,
    reason_codes text[] NOT NULL,
    policy_version text NOT NULL,
    decided_at timestamptz NOT NULL
);

CREATE TABLE IF NOT EXISTS governance.execution_attempts (
    attempt_id uuid PRIMARY KEY,
    proposal_id uuid NOT NULL REFERENCES governance.reorder_proposals(proposal_id),
    operation_id uuid NOT NULL,
    status text NOT NULL,
    message text NOT NULL,
    attempted_at timestamptz NOT NULL
);
"#
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn schema_contains_governance_tables_and_constraints() {
        let sql = governance_schema_sql();
        for table in [
            "agent_evaluations",
            "reorder_simulations",
            "reorder_proposals",
            "policy_decisions",
            "execution_attempts",
        ] {
            assert!(sql.contains(table));
        }
        assert!(sql.contains("CREATE SCHEMA IF NOT EXISTS governance"));
        assert!(sql.contains("CHECK (quantity >= 0)"));
    }

    #[test]
    fn parses_aspire_npgsql_connection_strings_for_tokio_postgres() {
        let config =
            parse_npgsql_connection_string("Host=localhost;Port=5432;Database=governancedb;Username=postgres;Password=secret;Include Error Detail=true")
                .unwrap();

        let debug = format!("{config:?}");
        assert!(debug.contains("user: Some(\"postgres\")"));
        assert!(debug.contains("dbname: Some(\"governancedb\")"));
        assert!(debug.contains("Tcp(\"localhost\")"));
        assert!(debug.contains("port: [5432]"));
    }
}
