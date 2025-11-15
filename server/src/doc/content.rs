use std::collections::{HashMap, HashSet};

use serde_json::{Map as JsonMap, Value as JsonValue};
use yrs::types::ToJson;
use yrs::updates::decoder::Decode;
use yrs::{Any, Doc as YrsDoc, Map, ReadTxn, Transact, Update};

const DEFAULT_SUMMARY_LIMIT: usize = 150;

type BlockMap = JsonMap<String, JsonValue>;

#[derive(Debug, Clone)]
pub(crate) struct SnapshotPageContent {
    pub(crate) title: String,
    pub(crate) summary: String,
}

#[derive(Debug, Clone)]
pub(crate) struct SnapshotMarkdown {
    pub(crate) title: String,
    pub(crate) markdown: String,
}

pub(crate) fn parse_doc_content(
    snapshot: &[u8],
    full_summary: bool,
) -> Option<SnapshotPageContent> {
    let graph = DocBlocks::build(snapshot)?;
    let mut summary = String::new();
    let mut queue = Vec::new();
    let mut visited = HashSet::new();
    let mut budget = if full_summary {
        SummaryBudget::unlimited()
    } else {
        SummaryBudget::limited(DEFAULT_SUMMARY_LIMIT)
    };

    queue.push(graph.root_id.clone());

    while let Some(block_id) = queue.pop() {
        if !visited.insert(block_id.clone()) {
            continue;
        }

        let Some(block) = graph.block(&block_id) else {
            continue;
        };

        match block_flavour(block).as_str() {
            "affine:page" | "affine:note" => push_children(block, &mut queue),
            "affine:attachment" | "affine:transcription" | "affine:callout" => {
                if full_summary {
                    push_children(block, &mut queue);
                }
            }
            "affine:table" => {
                if full_summary {
                    append_table_contents(block, &mut summary);
                }
            }
            "affine:paragraph" | "affine:list" | "affine:code" => {
                push_children(block, &mut queue);
                if budget.can_append() {
                    if let Some(text) = block_prop_string(block, "text") {
                        budget.append(&mut summary, &text);
                    }
                }
            }
            _ => {}
        }
    }

    Some(SnapshotPageContent {
        title: graph.title.clone(),
        summary,
    })
}

pub(crate) fn parse_doc_markdown(workspace_id: &str, snapshot: &[u8]) -> Option<SnapshotMarkdown> {
    let graph = DocBlocks::build(snapshot)?;
    let mut markdown = String::new();
    if !graph.title.is_empty() {
        markdown.push_str("# ");
        markdown.push_str(&graph.title);
        markdown.push('\n');
    }

    let mut state = MarkdownState::default();
    if let Some(root) = graph.root_block() {
        for child in block_children(root) {
            append_block_markdown(&graph, workspace_id, &child, 0, &mut markdown, &mut state);
        }
    }

    Some(SnapshotMarkdown {
        title: graph.title.clone(),
        markdown,
    })
}

#[derive(Default, Clone)]
struct MarkdownState {
    numbered_count: usize,
}

impl MarkdownState {
    fn reset_numbering(&mut self) {
        self.numbered_count = 0;
    }

    fn next_number(&mut self) -> usize {
        self.numbered_count += 1;
        self.numbered_count
    }
}

struct DocBlocks {
    blocks: HashMap<String, BlockMap>,
    root_id: String,
    title: String,
}

impl DocBlocks {
    fn build(snapshot: &[u8]) -> Option<Self> {
        let doc = YrsDoc::new();
        if !snapshot.is_empty() {
            let update = Update::decode_v1(snapshot).ok()?;
            let mut txn = doc.transact_mut();
            txn.apply_update(update).ok()?;
        }

        let txn = doc.transact();
        let blocks_map = txn.get_map("blocks")?;

        let mut blocks = HashMap::new();
        let mut root_id: Option<String> = None;
        let mut first_id: Option<String> = None;
        let mut title = String::new();

        for (_, value) in blocks_map.iter(&txn) {
            let json_any: Any = value.to_json(&txn);
            let json_value = serde_json::to_value(&json_any).ok()?;
            let Some(map) = json_value.as_object() else {
                continue;
            };
            let block_map = map.clone();

            let Some(id) = get_string(&block_map, "sys:id") else {
                continue;
            };

            if first_id.is_none() {
                first_id = Some(id.clone());
            }

            if root_id.is_none() {
                if let Some(flavour) = get_string(&block_map, "sys:flavour") {
                    if flavour == "affine:page" {
                        root_id = Some(id.clone());
                        title = block_prop_string(&block_map, "title").unwrap_or_default();
                    }
                }
            }

            blocks.insert(id, block_map);
        }

        let root_id = root_id.or(first_id)?;
        if title.is_empty() {
            if let Some(root) = blocks.get(&root_id) {
                title = block_prop_string(root, "title").unwrap_or_default();
            }
        }

        Some(Self {
            blocks,
            root_id,
            title,
        })
    }

    fn block(&self, id: &str) -> Option<&BlockMap> {
        self.blocks.get(id)
    }

    fn root_block(&self) -> Option<&BlockMap> {
        self.block(&self.root_id)
    }
}

fn append_block_markdown(
    graph: &DocBlocks,
    workspace_id: &str,
    block_id: &str,
    depth: usize,
    output: &mut String,
    state: &mut MarkdownState,
) {
    let Some(block) = graph.block(block_id) else {
        return;
    };

    match block_flavour(block).as_str() {
        "affine:page" | "affine:note" | "affine:frame" | "affine:surface" => {
            state.reset_numbering();
            for child in block_children(block) {
                append_block_markdown(graph, workspace_id, &child, depth, output, state);
            }
        }
        "affine:list" => {
            if let Some(text) = block_prop_string(block, "text") {
                let indent = "  ".repeat(depth);
                match block_prop_string(block, "type").as_deref() {
                    Some("numbered") => {
                        let index = state.next_number();
                        output.push_str(&format!("{indent}{index}. {text}\n"));
                    }
                    Some("todo") => {
                        let checked = block_prop_bool(block, "checked").unwrap_or(false);
                        let marker = if checked { "x" } else { " " };
                        output.push_str(&format!("{indent}[{marker}] {text}\n"));
                    }
                    _ => {
                        state.reset_numbering();
                        output.push_str(&format!("{indent}- {text}\n"));
                    }
                }
            }

            for child in block_children(block) {
                append_block_markdown(graph, workspace_id, &child, depth + 1, output, state);
            }
        }
        "affine:paragraph" => {
            state.reset_numbering();
            if let Some(text) = block_prop_string(block, "text") {
                let block_type =
                    block_prop_string(block, "type").unwrap_or_else(|| "text".to_string());
                match block_type.as_str() {
                    "quote" => {
                        output.push_str("> ");
                        output.push_str(&text);
                        output.push('\n');
                    }
                    ty if ty.starts_with('h') => {
                        if let Ok(level) = ty[1..].parse::<usize>() {
                            let level = level.clamp(1, 6);
                            output.push_str(&"#".repeat(level));
                            output.push(' ');
                            output.push_str(&text);
                            output.push('\n');
                        } else {
                            output.push_str(&text);
                            output.push('\n');
                        }
                    }
                    _ => {
                        output.push_str(&text);
                        output.push('\n');
                    }
                }
            }
        }
        "affine:code" => {
            state.reset_numbering();
            if let Some(text) = block_prop_string(block, "text") {
                output.push_str("```");
                if let Some(language) = block_prop_string(block, "language") {
                    if !language.is_empty() {
                        output.push(' ');
                        output.push_str(&language);
                    }
                }
                output.push('\n');
                output.push_str(&text);
                if !text.ends_with('\n') {
                    output.push('\n');
                }
                output.push_str("```\n");
            }
        }
        "affine:divider" => {
            state.reset_numbering();
            output.push_str("---\n");
        }
        "affine:embed" => {
            state.reset_numbering();
            if block_prop_string(block, "type").as_deref() == Some("image") {
                if let Some(source_id) = block_prop_string(block, "sourceId") {
                    output.push_str("![](/");
                    output.push_str(workspace_id);
                    output.push_str("/blobs/");
                    output.push_str(&source_id);
                    output.push_str(")\n");
                }
            }
        }
        _ => {
            if let Some(text) = block_prop_string(block, "text") {
                state.reset_numbering();
                output.push_str(&text);
                output.push('\n');
            }
        }
    }
}

fn push_children(block: &BlockMap, queue: &mut Vec<String>) {
    let mut children = block_children(block);
    while let Some(child) = children.pop() {
        queue.push(child);
    }
}

fn block_children(block: &BlockMap) -> Vec<String> {
    block
        .get("sys:children")
        .and_then(JsonValue::as_array)
        .map(|array| array.iter().filter_map(value_to_string).collect::<Vec<_>>())
        .unwrap_or_default()
}

fn block_flavour(block: &BlockMap) -> String {
    get_string(block, "sys:flavour").unwrap_or_default()
}

fn block_prop_string(block: &BlockMap, prop: &str) -> Option<String> {
    let key = format!("prop:{prop}");
    block.get(&key).and_then(value_to_string)
}

fn block_prop_bool(block: &BlockMap, prop: &str) -> Option<bool> {
    let key = format!("prop:{prop}");
    block.get(&key).and_then(value_to_bool)
}

fn get_string(block: &BlockMap, key: &str) -> Option<String> {
    block.get(key).and_then(value_to_string)
}

fn value_to_string(value: &JsonValue) -> Option<String> {
    match value {
        JsonValue::String(text) => Some(text.clone()),
        JsonValue::Number(num) => Some(num.to_string()),
        JsonValue::Bool(flag) => Some(flag.to_string()),
        JsonValue::Array(values) => {
            let mut parts = Vec::new();
            for value in values {
                if let Some(part) = value_to_string(value) {
                    parts.push(part);
                }
            }
            if parts.is_empty() {
                None
            } else {
                Some(parts.join(""))
            }
        }
        _ => None,
    }
}

fn value_to_bool(value: &JsonValue) -> Option<bool> {
    match value {
        JsonValue::Bool(flag) => Some(*flag),
        JsonValue::Number(num) => {
            if let Some(int) = num.as_i64() {
                Some(int != 0)
            } else if let Some(float) = num.as_f64() {
                Some(float != 0.0)
            } else {
                None
            }
        }
        JsonValue::String(text) => Some(text.eq_ignore_ascii_case("true")),
        _ => None,
    }
}

fn append_table_contents(block: &BlockMap, summary: &mut String) {
    let mut parts = Vec::new();
    for (key, value) in block.iter() {
        if key.starts_with("prop:cells.") && key.ends_with(".text") {
            if let Some(text) = value_to_string(value) {
                if !text.is_empty() {
                    parts.push(text);
                }
            }
        }
    }

    if !parts.is_empty() {
        summary.push_str(&parts.join("|"));
    }
}

enum SummaryBudget {
    Unlimited,
    Limited(usize),
}

impl SummaryBudget {
    fn unlimited() -> Self {
        Self::Unlimited
    }

    fn limited(limit: usize) -> Self {
        Self::Limited(limit)
    }

    fn can_append(&self) -> bool {
        match self {
            SummaryBudget::Unlimited => true,
            SummaryBudget::Limited(remaining) => *remaining > 0,
        }
    }

    fn append(&mut self, target: &mut String, text: &str) {
        if text.is_empty() {
            return;
        }

        match self {
            SummaryBudget::Unlimited => target.push_str(text),
            SummaryBudget::Limited(remaining) => {
                if *remaining == 0 {
                    return;
                }
                target.push_str(text);
                let len = text.chars().count();
                *remaining = remaining.saturating_sub(len);
            }
        }
    }
}
