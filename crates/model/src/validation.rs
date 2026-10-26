use crate::{Error, Result, DIMENSIONS};

/// Preserve the existing FP32 final normalization, rejecting overflow and NaN.
pub fn normalize(values: &[f32]) -> Result<[f32; DIMENSIONS]> {
    if values.len() != DIMENSIONS || values.iter().any(|x| !x.is_finite()) {
        return Err(Error::InvalidVector);
    }
    let squared = values.iter().fold(0.0_f32, |sum, x| sum + x * x);
    if !squared.is_finite() || squared <= 0.0 {
        return Err(Error::InvalidVector);
    }
    let norm = squared.sqrt();
    if !norm.is_finite() || norm <= 0.0 {
        return Err(Error::InvalidVector);
    }
    let mut result = [0.0; DIMENSIONS];
    for (out, value) in result.iter_mut().zip(values) {
        *out = *value / norm;
        if !out.is_finite() {
            return Err(Error::InvalidVector);
        }
    }
    Ok(result)
}

/// Strict validation helper; NaN never becomes a passing zero error.
pub fn cosine_error(a: &[f32], b: &[f32]) -> Result<f64> {
    if a.len() != DIMENSIONS || b.len() != DIMENSIONS || a.iter().chain(b).any(|x| !x.is_finite()) {
        return Err(Error::InvalidVector);
    }
    let aa: f64 = a.iter().map(|x| (*x as f64).powi(2)).sum();
    let bb: f64 = b.iter().map(|x| (*x as f64).powi(2)).sum();
    let dot: f64 = a.iter().zip(b).map(|(x, y)| *x as f64 * *y as f64).sum();
    let denominator = aa.sqrt() * bb.sqrt();
    if !aa.is_finite()
        || !bb.is_finite()
        || !dot.is_finite()
        || !denominator.is_finite()
        || denominator <= 0.0
    {
        return Err(Error::InvalidVector);
    }
    let cosine = dot / denominator;
    if !cosine.is_finite() {
        return Err(Error::InvalidVector);
    }
    let error = 1.0 - cosine.clamp(-1.0, 1.0);
    if !error.is_finite() {
        return Err(Error::InvalidVector);
    }
    Ok(error)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nonfinite_zero_and_overflow_never_pass() {
        for bad in [0.0, f32::NAN, f32::INFINITY, f32::NEG_INFINITY, f32::MAX] {
            let vector = [bad; DIMENSIONS];
            assert!(normalize(&vector).is_err());
        }
        let good = [1.0; DIMENSIONS];
        assert!(cosine_error(&[f32::NAN; DIMENSIONS], &good).is_err());
        assert!(cosine_error(&[0.0; DIMENSIONS], &good).is_err());
        assert!(cosine_error(&[f32::INFINITY; DIMENSIONS], &good).is_err());
        assert!(cosine_error(&good, &good).unwrap() <= f64::EPSILON);
    }
}
