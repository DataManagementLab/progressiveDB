SqlPrepareTable SqlPrepareTable() :
{
    final SqlIdentifier id;
    final Span s;
}
{
    <PREPARE> <TABLE> id = CompoundIdentifier() {
        s = span();
        return new SqlPrepareTable(s.end(id), id);
    }
}

boolean IfExistsOpt() :
{
}
{
    <IF> <EXISTS> { return true; }
|
    { return false; }
}

SqlNode ProgressiveOrderedQueryOrExpr() :
{
    SqlNode e;
    SqlNodeList orderBy = null;
    SqlNode start = null;
    SqlNode count = null;
}
{
    (
        e = ProgressiveQueryOrExpr(ExprContext.ACCEPT_QUERY)
    )
    [
        // use the syntactic type of the expression we just parsed
        // to decide whether ORDER BY makes sense
        orderBy = OrderBy(e.isA(SqlKind.QUERY))
    ]
    [
        // Postgres-style syntax. "LIMIT ... OFFSET ..."
        <LIMIT>
        (
            // MySQL-style syntax. "LIMIT start, count"
            start = UnsignedNumericLiteralOrParam()
            <COMMA> count = UnsignedNumericLiteralOrParam() {
                if (!this.conformance.isLimitStartCountAllowed()) {
                    throw new ParseException(RESOURCE.limitStartCountNotAllowed().str());
                }
            }
        |
            count = UnsignedNumericLiteralOrParam()
        |
            <ALL>
        )
    ]
    [
        // ROW or ROWS is required in SQL:2008 but we make it optional
        // because it is not present in Postgres-style syntax.
        // If you specify both LIMIT start and OFFSET, OFFSET wins.
        <OFFSET> start = UnsignedNumericLiteralOrParam() [ <ROW> | <ROWS> ]
    ]
    [
        // SQL:2008-style syntax. "OFFSET ... FETCH ...".
        // If you specify both LIMIT and FETCH, FETCH wins.
        <FETCH> ( <FIRST> | <NEXT> ) count = UnsignedNumericLiteralOrParam()
        ( <ROW> | <ROWS> ) <ONLY>
    ]
    {
        if (orderBy != null || start != null || count != null) {
            if (orderBy == null) {
                orderBy = SqlNodeList.EMPTY;
            }
            e = new SqlOrderBy(getPos(), e, orderBy, start, count);

        }
        return e;
    }
}

SqlNode FutureOrderedQueryOrExpr(ExprContext exprContext) :
{
    SqlNode e;
    SqlNodeList orderBy = null;
    SqlNode start = null;
    SqlNode count = null;
}
{
    (
        e = FutureQueryOrExpr(exprContext)
    )
    [
        // use the syntactic type of the expression we just parsed
        // to decide whether ORDER BY makes sense
        orderBy = OrderBy(e.isA(SqlKind.QUERY))
    ]
    [
        // Postgres-style syntax. "LIMIT ... OFFSET ..."
        <LIMIT>
        (
            // MySQL-style syntax. "LIMIT start, count"
            start = UnsignedNumericLiteralOrParam()
            <COMMA> count = UnsignedNumericLiteralOrParam() {
                if (!this.conformance.isLimitStartCountAllowed()) {
                    throw new ParseException(RESOURCE.limitStartCountNotAllowed().str());
                }
            }
        |
            count = UnsignedNumericLiteralOrParam()
        |
            <ALL>
        )
    ]
    [
        // ROW or ROWS is required in SQL:2008 but we make it optional
        // because it is not present in Postgres-style syntax.
        // If you specify both LIMIT start and OFFSET, OFFSET wins.
        <OFFSET> start = UnsignedNumericLiteralOrParam() [ <ROW> | <ROWS> ]
    ]
    [
        // SQL:2008-style syntax. "OFFSET ... FETCH ...".
        // If you specify both LIMIT and FETCH, FETCH wins.
        <FETCH> ( <FIRST> | <NEXT> ) count = UnsignedNumericLiteralOrParam()
        ( <ROW> | <ROWS> ) <ONLY>
    ]
    {
        if (orderBy != null || start != null || count != null) {
            if (orderBy == null) {
                orderBy = SqlNodeList.EMPTY;
            }
            e = new SqlOrderBy(getPos(), e, orderBy, start, count);

        }
        return e;
    }
}

SqlNode ProgressiveQueryOrExpr(ExprContext exprContext) :
{
    SqlNodeList withList = null;
    SqlNode e;
    SqlOperator op;
    SqlParserPos pos;
    SqlParserPos withPos;
    List<Object> list;
}
{
    [
        withList = WithList()
    ]
    e = ProgressiveLeafQueryOrExpr(exprContext) {
        list = startList(e);
    }
    (
        {
            if (!e.isA(SqlKind.QUERY)) {
                // whoops, expression we just parsed wasn't a query,
                // but we're about to see something like UNION, so
                // force an exception retroactively
                checkNonQueryExpression(ExprContext.ACCEPT_QUERY);
            }
        }
        op = BinaryQueryOperator() {
            // ensure a query is legal in this context
            pos = getPos();
            checkQueryExpression(exprContext);

        }
        e = ProgressiveLeafQueryOrExpr(ExprContext.ACCEPT_QUERY) {
            list.add(new SqlParserUtil.ToTreeListItem(op, pos));
            list.add(e);
        }
    )*
    {
        e = SqlParserUtil.toTree(list);
        if (withList != null) {
            e = new SqlWith(withList.getParserPosition(), withList, e);
        }
        return e;
    }
}

SqlNode FutureQueryOrExpr(ExprContext exprContext) :
{
    SqlNodeList withList = null;
    SqlNode e;
    SqlOperator op;
    SqlParserPos pos;
    SqlParserPos withPos;
    List<Object> list;
}
{
    [
        withList = WithList()
    ]
    e = FutureLeafQueryOrExpr(exprContext) {
        list = startList(e);
    }
    (
        {
            if (!e.isA(SqlKind.QUERY)) {
                // whoops, expression we just parsed wasn't a query,
                // but we're about to see something like UNION, so
                // force an exception retroactively
                checkNonQueryExpression(ExprContext.ACCEPT_QUERY);
            }
        }
        op = BinaryQueryOperator() {
            // ensure a query is legal in this context
            pos = getPos();
            checkQueryExpression(exprContext);

        }
        e = FutureLeafQueryOrExpr(ExprContext.ACCEPT_QUERY) {
            list.add(new SqlParserUtil.ToTreeListItem(op, pos));
            list.add(e);
        }
    )*
    {
        e = SqlParserUtil.toTree(list);
        if (withList != null) {
            e = new SqlWith(withList.getParserPosition(), withList, e);
        }
        return e;
    }
}

SqlNode ProgressiveLeafQueryOrExpr(ExprContext exprContext) :
{
    SqlNode e;
}
{
    e = Expression(exprContext) { return e; }
|
    e = ProgressiveLeafQuery(exprContext) { return e; }
}

SqlNode FutureLeafQueryOrExpr(ExprContext exprContext) :
{
    SqlNode e;
}
{
    e = FutureExpression(exprContext) { return e; }
|
    e = FutureLeafQuery(exprContext) { return e; }
}

SqlNode ProgressiveLeafQuery(ExprContext exprContext) :
{
    SqlNode e;
}
{
    {
        // ensure a query is legal in this context
        checkQueryExpression(exprContext);
    }
    e = SqlSelectProgressive() { return e; }
|
    e = TableConstructor() { return e; }
|
    e = ExplicitTable(getPos()) { return e; }
}

SqlNode FutureLeafQuery(ExprContext exprContext) :
{
    SqlNode e;
}
{
    {
        // ensure a query is legal in this context
        checkQueryExpression(exprContext);
    }
    e = SqlFutureSelect() { return e; }
|
    e = TableConstructor() { return e; }
|
    e = ExplicitTable(getPos()) { return e; }
}

SqlSelectProgressive SqlSelectProgressive() :
{
    final List<SqlLiteral> keywords = new ArrayList<SqlLiteral>();
    final SqlNodeList keywordList;
    List<SqlNode> selectList;
    final SqlNode fromClause;
    final SqlNodeList withFutureWhere;
    final SqlNode where;
    final SqlNodeList withFutureGroupBy;
    final SqlNodeList groupBy;
    final SqlNode having;
    final SqlNodeList windowDecls;
    final Span s;
}
{
    <SELECT> <PROGRESSIVE>
    {
        s = span();
    }
    SqlSelectKeywords(keywords)
    (
        <STREAM> {
            keywords.add(SqlSelectKeyword.STREAM.symbol(getPos()));
        }
    )?
    (
        <DISTINCT> {
            keywords.add(SqlSelectKeyword.DISTINCT.symbol(getPos()));
        }
    |   <ALL> {
            keywords.add(SqlSelectKeyword.ALL.symbol(getPos()));
        }
    )?
    {
        keywordList = new SqlNodeList(keywords, s.addAll(keywords).pos());
    }
    selectList = SelectFutureList()
    (
        <FROM> fromClause = FromClause()
        withFutureWhere = WithFutureWhereOpt()
        where = WhereOpt()
        withFutureGroupBy = WithFutureGroupByOpt()
        groupBy = GroupByOpt()
        having = HavingOpt()
        windowDecls = WindowOpt()
    |
        E() {
            fromClause = null;
            withFutureWhere = null;
            where = null;
            withFutureGroupBy = null;
            groupBy = null;
            having = null;
            windowDecls = null;
        }
    )
    {
        return new SqlSelectProgressive(s.end(this), keywordList,
            new SqlNodeList(selectList, Span.of(selectList).pos()),
            fromClause, withFutureWhere, where, withFutureGroupBy,
            groupBy, having, windowDecls, null, null, null);
    }
}

SqlSelect SqlFutureSelect() :
{
    final List<SqlLiteral> keywords = new ArrayList<SqlLiteral>();
    final SqlNodeList keywordList;
    List<SqlNode> selectList;
    final SqlNode fromClause;
    final SqlNode where;
    final SqlNodeList futureGroupBy;
    final SqlNode having;
    final SqlNodeList windowDecls;
    final Span s;
}
{
    <SELECT>
    {
        s = span();
    }
    SqlSelectKeywords(keywords)
    (
        <STREAM> {
            keywords.add(SqlSelectKeyword.STREAM.symbol(getPos()));
        }
    )?
    (
        <DISTINCT> {
            keywords.add(SqlSelectKeyword.DISTINCT.symbol(getPos()));
        }
    |   <ALL> {
            keywords.add(SqlSelectKeyword.ALL.symbol(getPos()));
        }
    )?
    {
        keywordList = new SqlNodeList(keywords, s.addAll(keywords).pos());
    }
    selectList = SelectFutureList()
    (
        <FROM> fromClause = FromClause()
        where = FutureWhereOpt()
        futureGroupBy = FutureGroupByOpt()
        having = HavingOpt()
        windowDecls = WindowOpt()
    |
        E() {
            fromClause = null;
            where = null;
            futureGroupBy = null;
            having = null;
            windowDecls = null;
        }
    )
    {
        return new SqlSelect(s.end(this), keywordList,
            new SqlNodeList(selectList, Span.of(selectList).pos()),
            fromClause, where, futureGroupBy, having, windowDecls, null, null, null);
    }
}

SqlNodeList WithFutureWhereOpt() :
{
    List<SqlNode> list = new ArrayList<SqlNode>();
    final Span s;
}
{
    LOOKAHEAD(3)
    <WITH> <FUTURE> <WHERE> { s = span(); }
    list = WithFutureWhereElementList() {
        return new SqlNodeList(list, s.addAll(list).pos());
    }
|
    {
        return null;
    }
}

SqlNodeList WithFutureGroupByOpt() :
{
    List<SqlNode> list = new ArrayList<SqlNode>();
    final Span s;
}
{
    <WITH> <FUTURE> <GROUP> { s = span(); }
    <BY> list = GroupingElementList() {
        return new SqlNodeList(list, s.addAll(list).pos());
    }
|
    {
        return null;
    }
}

List<SqlNode> SelectFutureList() :
{
    final List<SqlNode> list = new ArrayList<SqlNode>();
    SqlNode item;
}
{
    item = SelectFutureItem() {
        list.add(item);
    }
    (
        <COMMA> item = SelectFutureItem() {
            list.add(item);
        }
    )*
    {
        return list;
    }
}

SqlNode SelectFutureItem() :
{
    SqlNode e;
    SqlIdentifier id;
}
{
    e = SelectExpression()
    [
        <FUTURE> {
            e = new SqlFutureNode(e, getPos());
        }
    ]
    [
        [ <AS> ]
        id = SimpleIdentifier() {
            e = SqlStdOperatorTable.AS.createCall(span().end(e), e, id);
        }
    ]
    {
        return e;
    }
}

SqlNode FutureWhereOpt() :
{
    SqlNode condition;
}
{
    <WHERE> condition = FutureExpression(ExprContext.ACCEPT_SUB_QUERY)
    {
        return condition;
    }
    |
    {
        return null;
    }
}

SqlNode FutureExpression(ExprContext exprContext) :
{
    List<Object> list;
    SqlNode e;
}
{
    list = FutureExpression2(exprContext)
    {
        e = SqlParserUtil.toTree(list);
        return e;
    }
}

void FutureExpression2b(ExprContext exprContext, List<Object> list) :
{
    SqlNode e;
    SqlOperator op;
}
{
    (
        op = PrefixRowOperator() {
            checkNonQueryExpression(exprContext);
            list.add(new SqlParserUtil.ToTreeListItem(op, getPos()));
        }
    )*
    e = FutureExpression3(exprContext) {
        list.add(e);
    }
}

List<Object> FutureExpression2(ExprContext exprContext) :
{
    final List<Object> list = new ArrayList();
    List<Object> list2;
    SqlNodeList nodeList;
    SqlNode e;
    SqlOperator op;
    String p;
    final Span s = span();
}
{
    FutureExpression2b(exprContext, list)
    (
        (
            LOOKAHEAD(2)
            (
                // Special case for "IN", because RHS of "IN" is the only place
                // that an expression-list is allowed ("exp IN (exp1, exp2)").
                LOOKAHEAD(2) {
                    checkNonQueryExpression(exprContext);
                }
                (
                    <NOT> <IN> { op = SqlStdOperatorTable.NOT_IN; }
                |
                    <IN> { op = SqlStdOperatorTable.IN; }
                |
                    { final SqlKind k; }
                    k = comp()
                    (
                        <SOME> { op = SqlStdOperatorTable.some(k); }
                    |
                        <ANY> { op = SqlStdOperatorTable.some(k); }
                    |
                        <ALL> { op = SqlStdOperatorTable.all(k); }
                    )
                )
                { s.clear().add(this); }
                nodeList = ParenthesizedQueryOrCommaList(ExprContext.ACCEPT_NONCURSOR)
                {
                    list.add(new SqlParserUtil.ToTreeListItem(op, s.pos()));
                    s.add(nodeList);
                    // special case for stuff like IN (s1 UNION s2)
                    if (nodeList.size() == 1) {
                        SqlNode item = nodeList.get(0);
                        if (item.isA(SqlKind.QUERY)) {
                            list.add(item);
                        } else {
                            list.add(nodeList);
                        }
                    } else {
                        list.add(nodeList);
                    }
                }
            |
                LOOKAHEAD(2) {
                    checkNonQueryExpression(exprContext);
                }
                (
                    <NOT> <BETWEEN> {
                        op = SqlStdOperatorTable.NOT_BETWEEN;
                        s.clear().add(this);
                    }
                    [
                        <SYMMETRIC> { op = SqlStdOperatorTable.SYMMETRIC_NOT_BETWEEN; }
                    |
                        <ASYMMETRIC>
                    ]
                |
                    <BETWEEN>
                    {
                        op = SqlStdOperatorTable.BETWEEN;
                        s.clear().add(this);
                    }
                    [
                        <SYMMETRIC> { op = SqlStdOperatorTable.SYMMETRIC_BETWEEN; }
                    |
                        <ASYMMETRIC>
                    ]
                )
                e = Expression3(ExprContext.ACCEPT_SUB_QUERY) {
                    list.add(new SqlParserUtil.ToTreeListItem(op, s.pos()));
                    list.add(e);
                }
            |
                {
                    checkNonQueryExpression(exprContext);
                    s.clear().add(this);
                }
                (
                    <NOT>
                    (
                        <LIKE> { op = SqlStdOperatorTable.NOT_LIKE; }
                    |
                        <SIMILAR> <TO> { op = SqlStdOperatorTable.NOT_SIMILAR_TO; }
                    )
                |
                    <LIKE> { op = SqlStdOperatorTable.LIKE; }
                |
                    <SIMILAR> <TO> { op = SqlStdOperatorTable.SIMILAR_TO; }
                )
                list2 = Expression2(ExprContext.ACCEPT_SUB_QUERY) {
                    list.add(new SqlParserUtil.ToTreeListItem(op, s.pos()));
                    list.addAll(list2);
                }
                [
                    LOOKAHEAD(2)
                    <ESCAPE> e = Expression3(ExprContext.ACCEPT_SUB_QUERY) {
                        s.clear().add(this);
                        list.add(
                            new SqlParserUtil.ToTreeListItem(
                                SqlStdOperatorTable.ESCAPE, s.pos()));
                        list.add(e);
                    }
                ]
            |
                LOOKAHEAD(3) op = BinaryRowOperator() {
                    checkNonQueryExpression(exprContext);
                    list.add(new SqlParserUtil.ToTreeListItem(op, getPos()));
                }
                FutureExpression2b(ExprContext.ACCEPT_SUB_QUERY, list)
            |
                <LBRACKET>
                e = Expression(ExprContext.ACCEPT_SUB_QUERY)
                <RBRACKET> {
                    list.add(
                        new SqlParserUtil.ToTreeListItem(
                            SqlStdOperatorTable.ITEM, getPos()));
                    list.add(e);
                }
                (
                    <DOT>
                    p = Identifier() {
                        list.add(
                            new SqlParserUtil.ToTreeListItem(
                                SqlStdOperatorTable.DOT, getPos()));
                        list.add(new SqlIdentifier(p, getPos()));
                    }
                )*
            |
                {
                    checkNonQueryExpression(exprContext);
                }
                op = PostfixRowOperator() {
                    list.add(new SqlParserUtil.ToTreeListItem(op, getPos()));
                }
            )
        )+
        {
            return list;
        }
    |
        {
            return list;
        }
    )
}

SqlNode FutureExpression3(ExprContext exprContext) :
{
    final SqlNode e;
    final SqlNodeList list;
    final SqlNodeList list1;
    final SqlNodeList list2;
    final SqlOperator op;
    final Span s;
    Span rowSpan = null;
}
{
    LOOKAHEAD(2)
    e = AtomicRowExpression()
    {
        checkNonQueryExpression(exprContext);
        return e;
    }
|
    e = CursorExpression(exprContext) { return e; }
|
    LOOKAHEAD(3)
    <ROW> {
        s = span();
    }
    list = ParenthesizedSimpleIdentifierList() {
        if (exprContext != ExprContext.ACCEPT_ALL
            && exprContext != ExprContext.ACCEPT_CURSOR
            && !this.conformance.allowExplicitRowValueConstructor())
        {
            throw SqlUtil.newContextException(s.end(list),
                RESOURCE.illegalRowExpression());
        }
        return SqlStdOperatorTable.ROW.createCall(list);
    }
|
    [
        <ROW> { rowSpan = span(); }
    ]
    list1 = ParenthesizedFutureQueryOrCommaList(exprContext) {
        if (rowSpan != null) {
            // interpret as row constructor
            return SqlStdOperatorTable.ROW.createCall(rowSpan.end(list1),
                list1.toArray());
        }
    }
    [
        LOOKAHEAD(2)
        (
            e = IntervalQualifier()
            {
                if ((list1.size() == 1)
                    && list1.get(0) instanceof SqlCall)
                {
                    final SqlCall call = (SqlCall) list1.get(0);
                    if (call.getKind() == SqlKind.MINUS
                            && call.operandCount() == 2) {
                        List<SqlNode> list3 = startList(call.operand(0));
                        list3.add(call.operand(1));
                        list3.add(e);
                        return SqlStdOperatorTable.MINUS_DATE.createCall(
                            Span.of(list1).end(this), list3);
                     }
                }
                throw SqlUtil.newContextException(span().end(list1),
                    RESOURCE.illegalMinusDate());
            }
        )
    ]
    {
        if (list1.size() == 1) {
            // interpret as single value or query
            return list1.get(0);
        } else {
            // interpret as row constructor
            return SqlStdOperatorTable.ROW.createCall(span().end(list1),
                list1.toArray());
        }
    }
}

SqlNodeList ParenthesizedFutureQueryOrCommaList(
    ExprContext exprContext) :
{
    SqlNode e;
    List<SqlNode> list;
    ExprContext firstExprContext = exprContext;
    final Span s;
}
{
    <LPAREN>
    {
        // we've now seen left paren, so a query by itself should
        // be interpreted as a sub-query
        s = span();
        switch (exprContext) {
        case ACCEPT_SUB_QUERY:
            firstExprContext = ExprContext.ACCEPT_NONCURSOR;
            break;
        case ACCEPT_CURSOR:
            firstExprContext = ExprContext.ACCEPT_ALL;
            break;
        }
    }
    e = FutureOrderedQueryOrExpr(firstExprContext)
    {
        list = startList(e);
    }
    (
        <COMMA>
        {
            // a comma-list can't appear where only a query is expected
            checkNonQueryExpression(exprContext);
        }
        e = Expression(exprContext)
        {
            list.add(e);
        }
    )*
    <RPAREN>
    [
        <FUTURE> {
            list = startList(new SqlFutureNode(list.get(0), false, getPos()));
        }
    ]
    {
        return new SqlNodeList(list, s.end(this));
    }
}

SqlNodeList FutureGroupByOpt() :
{
    List<SqlNode> list = new ArrayList<SqlNode>();
    final Span s;
}
{
    <GROUP> { s = span(); }
    <BY> list = FutureGroupingElementList() {
        return new SqlNodeList(list, s.addAll(list).pos());
    }
|
    {
        return null;
    }
}

List<SqlNode> WithFutureWhereElementList() :
{
    List<SqlNode> list = new ArrayList<SqlNode>();
    SqlNode e;
}
{
    e = Expression(ExprContext.ACCEPT_NON_QUERY) { list.add(e); }
    (
        <COMMA>
        e = Expression(ExprContext.ACCEPT_NON_QUERY) { list.add(e); }
    )*
    { return list; }
}

List<SqlNode> FutureGroupingElementList() :
{
    List<SqlNode> list = new ArrayList<SqlNode>();
    SqlNode e;
}
{
    e = FutureGroupingElement() { list.add(e); }
    (
        <COMMA>
        e = FutureGroupingElement() { list.add(e); }
    )*
    { return list; }
}

SqlNode FutureGroupingElement() :
{
    List<SqlNode> list;
    final SqlNodeList nodes;
    final SqlNode e;
    final Span s;
}
{
    LOOKAHEAD(3)
    <LPAREN> <RPAREN> {
        return new SqlNodeList(getPos());
    }
|   e = Expression(ExprContext.ACCEPT_SUB_QUERY) <FUTURE> {
        return new SqlFutureNode(e, getPos());
    }
|   e = Expression(ExprContext.ACCEPT_SUB_QUERY) {
        return e;
    }
}

SqlCreate SqlCreateProgressiveView(Span s, boolean replace) :
{
    final SqlIdentifier id;
    SqlNodeList columnList = null;
    final SqlNode query;
}
{
    <PROGRESSIVE> <VIEW> id = CompoundIdentifier()
    [ columnList = ParenthesizedSimpleIdentifierList() ]
    <AS> query = FutureOrderedQueryOrExpr(ExprContext.ACCEPT_QUERY) {
        return new SqlCreateProgressiveView(s.end(this), replace, id, columnList, query);
    }
}

SqlDrop SqlDropProgressiveView(Span s, boolean replace) :
{
    final boolean ifExists;
    final SqlIdentifier id;
}
{
    <PROGRESSIVE> <VIEW> ifExists = IfExistsOpt() id = CompoundIdentifier() {
        return new SqlDropProgressiveView(s.end(this), ifExists, id);
    }
}

