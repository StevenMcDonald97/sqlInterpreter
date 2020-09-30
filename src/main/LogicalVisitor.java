package main;
import logical.*;

public interface LogicalVisitor {
	abstract void visit(DistinctOperator d);
	abstract void visit(JoinOperator j);
	abstract void visit(ProjectOperator p);
	abstract void visit(SelectOperator se);
	abstract void visit(SortOperator so);
	abstract void visit(TableOperator t);
}
