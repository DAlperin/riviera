pub trait TreeNode: Sized {
    fn apply<F>(&self, f: &mut F) -> Result<VisitRecursion, String>
    where
        F: FnMut(&Self) -> Result<VisitRecursion, String>,
    {
        match f(self)? {
            VisitRecursion::Continue => {}
            VisitRecursion::SkipChildren => return Ok(VisitRecursion::Continue),
            VisitRecursion::Stop => return Ok(VisitRecursion::Stop),
        };
        self.apply_children(f)
    }

    fn visit<V: TreeNodeVisitor<N = Self>>(
        &self,
        visitor: &mut V,
    ) -> Result<VisitRecursion, String> {
        match visitor.pre_visit(self)? {
            VisitRecursion::Continue => {}
            VisitRecursion::SkipChildren => return Ok(VisitRecursion::Continue),
            VisitRecursion::Stop => return Ok(VisitRecursion::Stop),
        };
        match self.apply_children(&mut |node| node.visit(visitor))? {
            VisitRecursion::Continue => {}
            VisitRecursion::SkipChildren => return Ok(VisitRecursion::Continue),
            VisitRecursion::Stop => return Ok(VisitRecursion::Stop),
        }
        visitor.post_visit(self)
    }

    fn transform_down<F>(&self, f: &mut F) -> Result<Self, String>
    where
        F: FnMut(&Self) -> Result<Self, String>,
    {
        let mut node = f(self)?;
        node = node.map_children(&mut |node| node.transform_down(f))?;
        Ok(node)
    }

    fn transform_up<F>(&self, f: &mut F) -> Result<Self, String>
    where
        F: FnMut(&Self) -> Result<Self, String>,
    {
        let mut node = self.map_children(&mut |node| node.transform_up(f))?;
        node = f(&node)?;
        Ok(node)
    }

    fn apply_children<F>(&self, f: &mut F) -> Result<VisitRecursion, String>
    where
        F: FnMut(&Self) -> Result<VisitRecursion, String>;

    fn map_children<F>(&self, f: &mut F) -> Result<Self, String>
    where
        F: FnMut(&Self) -> Result<Self, String>;
}

#[derive(Debug, Clone, PartialEq)]
pub enum VisitRecursion {
    Continue,
    SkipChildren,
    Stop,
}

pub trait TreeNodeVisitor {
    type N: TreeNode;

    fn pre_visit(&mut self, _node: &Self::N) -> Result<VisitRecursion, String> {
        Ok(VisitRecursion::Continue)
    }

    fn post_visit(&mut self, _node: &Self::N) -> Result<VisitRecursion, String> {
        Ok(VisitRecursion::Continue)
    }
}
