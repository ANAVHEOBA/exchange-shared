use argon2::{
    password_hash::{rand_core::OsRng, PasswordHash, PasswordHasher, PasswordVerifier, SaltString},
    Algorithm, Argon2, Params, Version,
};

// Tuned parameters: faster but still secure
// m=8MB, t=2 iterations, p=1 parallelism
fn get_argon2() -> Argon2<'static> {
    let params = Params::new(8192, 2, 1, None).unwrap();
    Argon2::new(Algorithm::Argon2id, Version::V0x13, params)
}

pub fn hash_password(password: &str) -> Result<String, argon2::password_hash::Error> {
    let salt = SaltString::generate(&mut OsRng);
    let argon2 = get_argon2();
    let hash = argon2.hash_password(password.as_bytes(), &salt)?;
    Ok(hash.to_string())
}

pub fn verify_password(password: &str, hash: &str) -> Result<bool, argon2::password_hash::Error> {
    let parsed_hash = PasswordHash::new(hash)?;
    Ok(get_argon2()
        .verify_password(password.as_bytes(), &parsed_hash)
        .is_ok())
}
