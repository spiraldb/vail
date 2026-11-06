//! Query-local lexical dependencies for placing list-loop invariants.
use crate::{expression_effects::{Effects, own_effect}, expressions::Expr};
use std::{collections::{HashMap, HashSet}, sync::Arc};

type Key = (usize, usize);
#[derive(Clone)]
pub(crate) struct Facts {
    dependencies: Arc<[u64]>,
    pub read: bool,
    pub effect: Effects,
}
struct Environment {
    parent: Option<usize>,
    definitions: Vec<usize>,
    lookup: HashMap<String, usize>,
}
pub(crate) struct Dependencies {
    environments: Vec<Environment>,
    facts: HashMap<Key, Facts>,
    children: HashMap<Key, Vec<usize>>,
    definitions: usize,
    available: Vec<u64>,
    retained_words: usize,
    work: HashSet<Key>,
    resolved: HashMap<usize,HashMap<String,Option<usize>>>,
    resolutions: usize,
    intrinsics:HashMap<String,usize>,
}
impl Dependencies {
    // This sidecar falls back to ordinary lowering rather than retaining an
    // unbounded node-context × definition matrix for pathological queries.
    const MAX_WORDS: usize = 1_048_576;
    const MAX_CONTEXTS: usize = 65_536;
    pub fn new(available: impl Iterator<Item = String>, parameter: &str) -> Option<Self> {
        let mut definitions = Vec::new();
        let mut lookup = HashMap::new();
        for name in available {
            if definitions.len() >= Self::MAX_CONTEXTS { return None; }
            let id = definitions.len();
            definitions.push(id);
            lookup.insert(name,id);
        }
        let n = definitions.len();
        let mut mask = vec![u64::MAX; n.div_ceil(64)];
        if n % 64 != 0 { *mask.last_mut()? = (1u64 << (n % 64)) - 1; }
        let mut out = Self {
            environments: vec![Environment { parent: None, definitions, lookup }],
            facts: HashMap::new(), children: HashMap::new(), definitions: n,
            available: mask, retained_words: 0, work:HashSet::new(),resolved:HashMap::new(),resolutions:0,intrinsics:HashMap::new(),
        };
        out.bind(0, vec![parameter])?;
        Some(out)
    }
    /// Finalize callback bindings before analysis or resolution begins.
    pub fn add_parameter(&mut self, name: &str) -> Option<()> {
        if self.definitions >= Self::MAX_CONTEXTS { return None; }
        self.environments[1].definitions.push(self.definitions);
        // The element parameter shadows an identically named accumulator.
        self.environments[1].lookup.entry(name.to_owned()).or_insert(self.definitions);
        self.definitions += 1;
        Some(())
    }
    pub fn add_intrinsic(&mut self,name:&str,available:bool)->Option<()> {
        if self.definitions>=Self::MAX_CONTEXTS{return None;}
        let id=self.definitions;self.definitions+=1;
        self.intrinsics.insert(name.to_owned(),id);
        if available {self.available.resize(self.available.len().max(id/64+1),0);self.available[id/64]|=1u64<<(id%64);}
        Some(())
    }
    pub fn callback_context(&self) -> usize { 1 }
    fn bind(&mut self, parent: usize, names: Vec<&str>) -> Option<usize> {
        if names.is_empty() { return Some(parent); }
        if self.environments.len() >= Self::MAX_CONTEXTS || self.definitions + names.len() > Self::MAX_CONTEXTS { return None; }
        let mut definitions = Vec::with_capacity(names.len());
        let mut lookup = HashMap::new();
        for name in names {
            let id=self.definitions; self.definitions+=1;
            definitions.push(id); lookup.insert(name.to_owned(),id);
        }
        let id = self.environments.len();
        self.environments.push(Environment { parent: Some(parent), definitions, lookup });
        Some(id)
    }
    fn resolve(&mut self, context: usize, name: &str) -> Option<usize> {
        let mut current=context;
        let mut visited=Vec::new();
        let result=loop {
            if let Some(id)=self.resolved.get(&current).and_then(|names| names.get(name)) { break *id; }
            visited.push(current);
            let env=&self.environments[current];
            if let Some(id)=env.lookup.get(name) { break Some(*id); }
            let Some(parent)=env.parent else { break None; };
            current=parent;
        };
        if self.resolutions.checked_add(visited.len())? > Self::MAX_CONTEXTS * 4 { return None; }
        self.resolutions+=visited.len();
        for context in visited {
            self.resolved.entry(context).or_default().insert(name.to_owned(),result);
        }
        result
    }
    fn key(expr: &Expr, context: usize) -> Key { (expr as *const Expr as usize, context) }
    pub fn analyze(&mut self, expr: &Expr, context: usize) -> Option<Facts> {
        let key = Self::key(expr, context);
        if let Some(facts) = self.facts.get(&key) { return Some(facts.clone()); }
        if self.work.len() >= Self::MAX_CONTEXTS || !self.work.insert(key) { return None; }
        let mut dependencies = Vec::<u64>::new();
        if let Expr::LambdaVariable { name, .. } = expr {
            let id = self.resolve(context, name)?;
            dependencies.resize(id / 64 + 1, 0);
            dependencies[id / 64] |= 1u64 << (id % 64);
        }
        if let Expr::AlignedMapInput{axis,..}=expr {
            let id=*self.intrinsics.get(axis)?;
            dependencies.resize(id/64+1,0);dependencies[id/64]|=1u64<<(id%64);
        }
        let mut facts = Facts {
            dependencies: Arc::from([]),
            read: matches!(expr, Expr::Column { .. } | Expr::Global { .. } | Expr::ExternalGlobal { .. } | Expr::LambdaVariable { .. } | Expr::AlignedMapInput { .. }),
            effect: own_effect(expr),
        };
        let mut nodes = Vec::new();
        expr.for_each_child(&mut |child| nodes.push(child));
        let mut contexts = Vec::with_capacity(nodes.len());
        for (position, child) in nodes.into_iter().enumerate() {
            let child_context = self.bind(context, expr.child_binders(position))?;
            let child = self.analyze(child, child_context)?;
            dependencies.resize(dependencies.len().max(child.dependencies.len()), 0);
            for (word, value) in dependencies.iter_mut().zip(child.dependencies.iter()) { *word |= value; }
            if child_context != context {
                // Definitions introduced on this edge are internal to expr.
                for id in &self.environments[child_context].definitions {
                    if let Some(word) = dependencies.get_mut(id / 64) { *word &= !(1u64 << (id % 64)); }
                }
            }
            facts.read |= child.read;
            facts.effect = facts.effect.max(child.effect);
            contexts.push(child_context);
        }
        while dependencies.last() == Some(&0) { dependencies.pop(); }
        self.retained_words = self.retained_words.checked_add(dependencies.len())?;
        if self.retained_words > Self::MAX_WORDS { return None; }
        facts.dependencies = dependencies.into();
        self.children.insert(key, contexts);
        self.facts.insert(key, facts.clone());
        Some(facts)
    }
    pub fn invariant(&self, expr: &Expr, context: usize) -> bool {
        let facts = &self.facts[&Self::key(expr, context)];
        facts.read && facts.effect == Effects::Pure && facts.dependencies.iter().enumerate()
            .all(|(i, word)| word & !self.available.get(i).copied().unwrap_or(0) == 0)
    }
    pub fn child_context(&self, expr: &Expr, context: usize, position: usize) -> usize {
        self.children[&Self::key(expr, context)][position]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn variable(name: &str) -> Expr {
        Expr::LambdaVariable { name: name.into(), dtype: None }
    }
    #[test]
    fn outer_definition_is_available_but_current_definition_is_not() {
        let mut analysis = Dependencies::new(["outer".to_owned()].into_iter(), "inner").unwrap();
        let context = analysis.callback_context();
        let outer = variable("outer");
        let inner = variable("inner");
        analysis.analyze(&outer, context).unwrap();
        analysis.analyze(&inner, context).unwrap();
        assert!(analysis.invariant(&outer, context));
        assert!(!analysis.invariant(&inner, context));
    }
    #[test]
    fn a_shadowed_definition_does_not_reuse_shared_node_facts() {
        let shared = Arc::new(variable("outer"));
        let nested = Expr::ArrayMap {
            value: shared.clone(), parameter: "outer".into(), body: shared.clone(),
        };
        let mut analysis = Dependencies::new(["outer".to_owned()].into_iter(), "inner").unwrap();
        let context = analysis.callback_context();
        analysis.analyze(&nested, context).unwrap();
        let body_context = analysis.child_context(&nested, context, 1);
        assert!(analysis.invariant(&shared, context));
        assert!(!analysis.invariant(&shared, body_context));
        assert!(analysis.invariant(&nested, context));
    }
    #[test]
    fn unused_binder_needs_no_dependency_word() {
        let nested = Expr::ArrayMap {
            value: Arc::new(Expr::Column { name: "values".into() }),
            parameter: "unused".into(),
            body: Arc::new(Expr::Column { name: "offset".into() }),
        };
        let mut analysis = Dependencies::new(std::iter::empty(), "inner").unwrap();
        let context = analysis.callback_context();
        analysis.analyze(&nested, context).unwrap();
        assert!(analysis.invariant(&nested, context));
    }
}
