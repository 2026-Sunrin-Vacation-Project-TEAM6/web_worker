use jsonwebtoken::{decode, Algorithm, DecodingKey, Validation};
use serde::Deserialize;
use sqlx::PgPool;

#[derive(Debug, Deserialize)]
struct Claims {
    sub: String,
}

pub fn verify_token(token: &str, secret: &str, algorithm: &str) -> Option<i64> {
    let alg = match algorithm {
        "HS384" => Algorithm::HS384,
        "HS512" => Algorithm::HS512,
        _ => Algorithm::HS256,
    };
    let data = decode::<Claims>(
        token,
        &DecodingKey::from_secret(secret.as_bytes()),
        &Validation::new(alg),
    )
    .ok()?;
    data.claims.sub.parse::<i64>().ok()
}

/// Returns true if `user_id` may access the workspace that owns `stack_box_id`,
/// either as the workspace owner or as a workspace member.
pub async fn can_access_stack_box(db: &PgPool, stack_box_id: i64, user_id: i64) -> bool {
    let result: Option<bool> = sqlx::query_scalar(
        "SELECT (w.owner_id = $2) OR EXISTS ( \
            SELECT 1 FROM workspace_members wm \
            WHERE wm.workspace_id = w.id AND wm.user_id = $2 \
         ) \
         FROM stack_boxes sb \
         JOIN workspaces w ON w.id = sb.workspace_id \
         WHERE sb.id = $1",
    )
    .bind(stack_box_id)
    .bind(user_id)
    .fetch_optional(db)
    .await
    .unwrap_or(None);

    result.unwrap_or(false)
}
