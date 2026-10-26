// Resampling algorithm derived from PyTorch2.14.1 CPU UpSampleKernel.cpp and
// UpSampleKernelAVXAntialias.h (BSD-3-Clause), which attribute Keys filtering to
// Pillow commit6812205f (HPND). Full notices:licenses/PyTorch-LICENSE.txt and
// licenses/Pillow-LICENSE.txt. No upstream model weights are distributed here.
use crate::{Error, Result};
use std::path::Path;

pub struct ImageInput {
    pub pixels: Vec<f32>,
    pub position_ids: Vec<i64>,
    pub max_patches: usize,
    pub soft_tokens: usize,
    pub resized_width: usize,
    pub resized_height: usize,
}

impl ImageInput {
    pub fn text_input(&self) -> crate::TextInput {
        crate::text::media_ids(255999, 258880, 258882, self.soft_tokens)
    }
}

/// Model-level RGB decoding. Ingest owns orientation, transparency-on-white,
/// original artifacts and codec-specific normalization before this boundary.
pub fn decode_rgb(path: &Path) -> Result<::image::RgbImage> {
    Ok(::image::ImageReader::open(path)?
        .with_guessed_format()?
        .decode()?
        .into_rgb8())
}

pub fn preprocess_rgb(width: usize, height: usize, rgb: &[u8], budget: u32) -> Result<ImageInput> {
    if ![70, 140, 280, 560, 1120].contains(&budget) {
        return Err(Error::InvalidInput(
            "image soft-token budget must be70,140,280,560or1120",
        ));
    }
    if width == 0
        || height == 0
        || width.checked_mul(height).and_then(|n| n.checked_mul(3)) != Some(rgb.len())
    {
        return Err(Error::InvalidInput(
            "image must contain exactly width*height*3RGBbytes",
        ));
    }
    let max_patches = budget as usize * 9;
    let target_pixels = max_patches * 256;
    let factor = (target_pixels as f64 / (height as f64 * width as f64)).sqrt();
    let mut new_height = ((factor * height as f64 / 48.0).floor() as usize) * 48;
    let mut new_width = ((factor * width as f64 / 48.0).floor() as usize) * 48;
    if new_height == 0 && new_width == 0 {
        return Err(Error::InvalidInput(
            "image aspect ratio cannot fit token budget",
        ));
    }
    let max_side = budget as usize * 48;
    if new_height == 0 {
        new_height = 48;
        new_width = ((width as f64 / height as f64).floor() as usize * 48).min(max_side);
    } else if new_width == 0 {
        new_width = 48;
        new_height = ((height as f64 / width as f64).floor() as usize * 48).min(max_side);
    }
    if new_height
        .checked_mul(new_width)
        .is_none_or(|n| n > target_pixels)
    {
        return Err(Error::InvalidInput("resized image exceeds token budget"));
    }
    let resized = resize_rgb(rgb, width, height, new_width, new_height);
    let patch_width = new_width / 16;
    let patch_height = new_height / 16;
    let valid_patches = patch_width * patch_height;
    let mut pixels = vec![0.0; max_patches * 768];
    let mut position_ids = vec![-1; max_patches * 2];
    for py in 0..patch_height {
        for px in 0..patch_width {
            let patch = py * patch_width + px;
            // Pinned processor uses meshgrid(width,height,indexing="xy").
            position_ids[patch * 2] = px as i64;
            position_ids[patch * 2 + 1] = py as i64;
            for y in 0..16 {
                for x in 0..16 {
                    for c in 0..3 {
                        pixels[patch * 768 + (y * 16 + x) * 3 + c] =
                            resized[((py * 16 + y) * new_width + px * 16 + x) * 3 + c] as f32
                                * (1.0_f32 / 255.0);
                    }
                }
            }
        }
    }
    Ok(ImageInput {
        pixels,
        position_ids,
        max_patches,
        soft_tokens: valid_patches / 9,
        resized_width: new_width,
        resized_height: new_height,
    })
}

struct Coefficients {
    first: usize,
    weights: Vec<i32>,
}

fn cubic(x: f64) -> f64 {
    let x = x.abs();
    let a = -0.5;
    if x < 1.0 {
        ((a + 2.0) * x - (a + 3.0)) * x * x + 1.0
    } else if x < 2.0 {
        ((a * x - 5.0 * a) * x + 8.0 * a) * x - 4.0 * a
    } else {
        0.0
    }
}

// Scalar equivalent of PyTorch2.14.1 CPU uint8 antialiased bicubic:
// UpSampleKernel.cpp753-794,864-1044 and horizontal/vertical uint8 loops.
// Float64 normalized Keys(-0.5) coefficients are quantized to signed16 bits,
// with the same axis-wide precision and intermediate uint8 rounding/clamp.
fn coefficients(input: usize, output: usize) -> (Vec<Coefficients>, u32) {
    let scale = input as f64 / output as f64;
    let support = 2.0 * scale.max(1.0);
    let max_size = support.ceil() as usize * 2 + 1;
    let mut floating = Vec::with_capacity(output);
    let mut max_weight = 0.0_f64;
    for i in 0..output {
        let center = scale * (i as f64 + 0.5);
        let first = ((center - support + 0.5) as i64).max(0) as usize;
        let end = ((center + support + 0.5) as usize).min(input);
        let size = end.saturating_sub(first).min(max_size);
        let inverse = 1.0 / scale.max(1.0);
        let mut weights: Vec<f64> = (0..size)
            .map(|j| cubic((j as f64 + first as f64 - center + 0.5) * inverse))
            .collect();
        let sum: f64 = weights.iter().sum();
        if sum != 0.0 {
            for value in &mut weights {
                *value /= sum;
                max_weight = max_weight.max(*value);
            }
        }
        floating.push((first, weights));
    }
    let mut precision = 0;
    while precision < 22 {
        if (0.5 + max_weight * ((1_u32 << (precision + 1)) as f64)) as i32 >= 1 << 15 {
            break;
        }
        precision += 1;
    }
    let coefficients = floating
        .into_iter()
        .map(|(first, weights)| Coefficients {
            first,
            weights: weights
                .into_iter()
                .map(|w| {
                    let v = w * (1_u32 << precision) as f64;
                    (if v < 0.0 { v - 0.5 } else { v + 0.5 }) as i32
                })
                .collect(),
        })
        .collect();
    (coefficients, precision)
}

fn resize_rgb(
    input: &[u8],
    width: usize,
    height: usize,
    new_width: usize,
    new_height: usize,
) -> Vec<u8> {
    let mut horizontal = if new_width == width {
        input.to_vec()
    } else {
        let (coefficients, p) = coefficients(width, new_width);
        let mut output = vec![0_u8; new_width * height * 3];
        for y in 0..height {
            for (x, coeff) in coefficients.iter().enumerate() {
                for c in 0..3 {
                    let mut sum = 1_i64 << (p - 1);
                    for (j, weight) in coeff.weights.iter().enumerate() {
                        sum += input[(y * width + coeff.first + j) * 3 + c] as i64 * *weight as i64;
                    }
                    output[(y * new_width + x) * 3 + c] = (sum >> p).clamp(0, 255) as u8;
                }
            }
        }
        output
    };
    if new_height == height {
        return horizontal;
    }
    let (coefficients, p) = coefficients(height, new_height);
    let mut output = vec![0_u8; new_width * new_height * 3];
    for (y, coeff) in coefficients.iter().enumerate() {
        for x in 0..new_width {
            for c in 0..3 {
                let mut sum = 1_i64 << (p - 1);
                for (j, weight) in coeff.weights.iter().enumerate() {
                    sum += horizontal[((coeff.first + j) * new_width + x) * 3 + c] as i64
                        * *weight as i64;
                }
                output[(y * new_width + x) * 3 + c] = (sum >> p).clamp(0, 255) as u8;
            }
        }
    }
    horizontal.clear();
    output
}
