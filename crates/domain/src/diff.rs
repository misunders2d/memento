//! Python SequenceMatcher(autojunk=False) behavior for immutable history words.
use serde_json::{Value, json};
use std::collections::{BTreeMap, HashMap};

fn tokens(text: &str) -> Vec<String> {
    let mut result: Vec<String> = Vec::new();
    let mut previous = 3;
    for letter in text.chars() {
        let kind = if crate::unicode::is_word(letter) {
            0
        } else if letter.is_whitespace() {
            1
        } else {
            2
        };
        if kind == previous {
            result.last_mut().expect("previous token").push(letter);
        } else {
            result.push(letter.to_string());
            previous = kind;
        }
    }
    result
}

pub fn word_diff(old: &str, new: &str) -> Vec<Value> {
    let a = tokens(old);
    let b = tokens(new);
    let mut positions: HashMap<&str, Vec<usize>> = HashMap::new();
    for (index, value) in b.iter().enumerate() {
        positions.entry(value).or_default().push(index);
    }
    let mut queue = vec![(0, a.len(), 0, b.len())];
    let mut blocks = Vec::new();
    while let Some((alo, ahi, blo, bhi)) = queue.pop() {
        let (mut best_i, mut best_j, mut best_size) = (alo, blo, 0);
        let mut lengths: BTreeMap<usize, usize> = BTreeMap::new();
        for i in alo..ahi {
            let mut next = BTreeMap::new();
            if let Some(candidates) = positions.get(a[i].as_str()) {
                for &j in candidates {
                    if j < blo {
                        continue;
                    }
                    if j >= bhi {
                        break;
                    }
                    let size = j
                        .checked_sub(1)
                        .and_then(|prior| lengths.get(&prior))
                        .copied()
                        .unwrap_or(0)
                        + 1;
                    next.insert(j, size);
                    if size > best_size {
                        (best_i, best_j, best_size) = (i + 1 - size, j + 1 - size, size);
                    }
                }
            }
            lengths = next;
        }
        if best_size > 0 {
            blocks.push((best_i, best_j, best_size));
            if alo < best_i && blo < best_j {
                queue.push((alo, best_i, blo, best_j));
            }
            if best_i + best_size < ahi && best_j + best_size < bhi {
                queue.push((best_i + best_size, ahi, best_j + best_size, bhi));
            }
        }
    }
    blocks.sort_unstable();
    let mut merged: Vec<(usize, usize, usize)> = Vec::new();
    for (ai, bi, size) in blocks {
        if let Some(last) = merged.last_mut() {
            if last.0 + last.2 == ai && last.1 + last.2 == bi {
                last.2 += size;
                continue;
            }
        }
        merged.push((ai, bi, size));
    }
    merged.push((a.len(), b.len(), 0));
    let (mut i, mut j) = (0, 0);
    let mut result = Vec::new();
    for (ai, bi, size) in merged {
        if i < ai || j < bi {
            let op = if i < ai && j < bi {
                "replace"
            } else if i < ai {
                "delete"
            } else {
                "insert"
            };
            result.push(json!({"op":op,"old":a[i..ai].join(""),"new":b[j..bi].join("")}));
        }
        i = ai + size;
        j = bi + size;
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn exact_refinement_diff() {
        assert_eq!(
            word_diff(
                "The report is pending.",
                "The launch report is pending legal approval."
            ),
            vec![
                json!({"op":"insert","old":"","new":" launch"}),
                json!({"op":"insert","old":"","new":" legal approval"})
            ]
        );
    }
}
