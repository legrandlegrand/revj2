BEGIN {
  int x0, y0;
  int x1, y1;
  edge_t e;
  node_t t, h;
  graph_t ng = graph ("G","D");
  $tvtype = TV_ne;
}
N {
  clone (ng, $);
}
E{
  t = node (ng, $.tail.name);
  h = node (ng, $.head.name);
  sscanf ($.tail.pos, "%d,%d", &x0, &y0);
  sscanf ($.head.pos, "%d,%d", &x1, &y1);
  if (x0 < x1) {
    e = edge (t, h, "");
    copyA($,e);
    e.tailport = $.tailport + ':e';
    e.headport = $.headport + ':w';
    e.arrowhead = $.arrowhead;
	e.arrowtail = $.arrowtail;
	e.color = $.color;
  }
  else {
    e = edge (h, t, "");
    copyA($,e);
    e.tailport = $.headport + ':e';
    e.headport = $.tailport + ':w';
    e.arrowhead = $.arrowtail;
    e.arrowtail = $.arrowhead;
    e.color = $.color;
  }
}
END {
  write (ng);
}