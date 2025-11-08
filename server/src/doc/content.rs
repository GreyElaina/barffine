use std::collections::{HashMap, HashSet};

use y_octo::{Any, Doc as YoctoDoc, Map, Value};

const DEFAULT_SUMMARY_LIMIT: usize = 150;

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
    blocks: HashMap<String, Map>,
    root_id: String,
    title: String,
}

impl DocBlocks {
    fn build(snapshot: &[u8]) -> Option<Self> {
        let doc = YoctoDoc::try_from_binary_v1(snapshot).ok()?;
        let blocks_map = doc.get_map("blocks").ok()?;

        let mut blocks = HashMap::new();
        let mut root_id: Option<String> = None;
        let mut first_id: Option<String> = None;
        let mut title = String::new();

        for (_, value) in blocks_map.iter() {
            let Some(map) = value.to_map() else {
                continue;
            };

            let Some(id) = get_string(&map, "sys:id") else {
                continue;
            };

            if first_id.is_none() {
                first_id = Some(id.clone());
            }

            if root_id.is_none() {
                if let Some(flavour) = get_string(&map, "sys:flavour") {
                    if flavour == "affine:page" {
                        root_id = Some(id.clone());
                        title = block_prop_string(&map, "title").unwrap_or_default();
                    }
                }
            }

            blocks.insert(id, map);
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

    fn block(&self, id: &str) -> Option<&Map> {
        self.blocks.get(id)
    }

    fn root_block(&self) -> Option<&Map> {
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

fn push_children(block: &Map, queue: &mut Vec<String>) {
    let mut children = block_children(block);
    while let Some(child) = children.pop() {
        queue.push(child);
    }
}

fn block_children(block: &Map) -> Vec<String> {
    match block.get("sys:children") {
        Some(Value::Array(array)) => array
            .iter()
            .filter_map(|value| value_to_string(&value))
            .collect(),
        Some(Value::Any(Any::Array(values))) => values.iter().filter_map(any_to_string).collect(),
        _ => Vec::new(),
    }
}

fn block_flavour(block: &Map) -> String {
    get_string(block, "sys:flavour").unwrap_or_default()
}

fn block_prop_string(block: &Map, prop: &str) -> Option<String> {
    let key = format!("prop:{prop}");
    block.get(&key).and_then(|value| value_to_string(&value))
}

fn block_prop_bool(block: &Map, prop: &str) -> Option<bool> {
    let key = format!("prop:{prop}");
    block.get(&key).and_then(|value| value_to_bool(&value))
}

fn get_string(block: &Map, key: &str) -> Option<String> {
    block.get(key).and_then(|value| value_to_string(&value))
}

fn value_to_string(value: &Value) -> Option<String> {
    match value {
        Value::Text(text) => Some(text.to_string()),
        Value::Any(any) => any_to_string(any),
        Value::Array(array) => {
            let joined: Vec<_> = array
                .iter()
                .filter_map(|value| value_to_string(&value))
                .collect();
            if joined.is_empty() {
                None
            } else {
                Some(joined.join(""))
            }
        }
        _ => None,
    }
}

fn value_to_bool(value: &Value) -> Option<bool> {
    match value {
        Value::Any(Any::True) => Some(true),
        Value::Any(Any::False) => Some(false),
        Value::Any(Any::Integer(int)) => Some(*int != 0),
        Value::Any(Any::BigInt64(int)) => Some(*int != 0),
        Value::Any(Any::Float32(float)) => Some(float.into_inner() != 0.0),
        Value::Any(Any::Float64(float)) => Some(float.into_inner() != 0.0),
        Value::Any(Any::String(value)) => Some(value.eq_ignore_ascii_case("true")),
        _ => None,
    }
}

fn any_to_string(any: &Any) -> Option<String> {
    match any {
        Any::String(value) => Some(value.clone()),
        Any::Integer(value) => Some(value.to_string()),
        Any::Float32(value) => Some(value.into_inner().to_string()),
        Any::Float64(value) => Some(value.into_inner().to_string()),
        Any::BigInt64(value) => Some(value.to_string()),
        Any::True => Some("true".into()),
        Any::False => Some("false".into()),
        Any::Array(values) => {
            let joined: Vec<_> = values.iter().filter_map(any_to_string).collect();
            if joined.is_empty() {
                None
            } else {
                Some(joined.join(""))
            }
        }
        _ => None,
    }
}

fn append_table_contents(block: &Map, summary: &mut String) {
    let mut parts = Vec::new();
    for key in block.keys() {
        if key.starts_with("prop:cells.") && key.ends_with(".text") {
            if let Some(value) = block.get(key) {
                if let Some(text) = value_to_string(&value) {
                    if !text.is_empty() {
                        parts.push(text);
                    }
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
