var width = 800,
  height = 600;

d3.json('domain.json', function (json) {
  var nodes = json.slice(0, 400);

  var maxCount = nodes[0].count;
  var minCount = nodes[nodes.length - 1].count;

  var scale = d3.scaleLog()
    .domain([minCount, maxCount])
    .range([width / 150, width / 15])
    .clamp(true);

  var color = d3.scaleOrdinal(d3.schemeCategory10);

  var svg = d3.select('svg')
    .attr('width', '100%')
    .attr('height', height);

  var nodeGroup = svg.append('g');

  svg.call(d3.zoom()
      .scaleExtent([1 / 2, 2])
      .on('zoom', function () {
        nodeGroup.attr('transform', d3.event.transform);
      }));

  // var tooltip = d3.select('body').append('div')
  //   .attr('class', 'tooltip')
  //   .style('position', 'absolute')
  //   .style('z-index', 1000)
  //   .style('visibility', 'hidden');

  // var domainTooltip = tooltip.append('p');
  // var occurenceTooltip = tooltip.append('p');
  var domainTooltip = d3.select('#tooltip-text')
  var occurenceTooltip = d3.select('#tooltip-occ')

  var simulation = d3.forceSimulation()
    .nodes(nodes)
    .force('collide', d3.forceCollide().radius(function (d) { return scale(d.count); }))
    .force('charge', d3.forceManyBody().strength(1))
    .force('center', d3.forceCenter(width / 2, height / 2));

  var nodeElements = nodeGroup.selectAll('.node')
    .data(nodes)
    .enter().append('a')
    .attr('class', 'node')
    .attr('target', '_blank')
    .attr('xlink:type', 'simple')
    .attr('xlink:href', function (d) { return 'http://' + d.domain; })
    .attr('xlink:show', 'new')
    .call(d3.drag()
      .on('start', function () {
        if (!d3.event.active) {
          simulation.alphaTarget(0.9).restart();
        }
      })
      .on('drag', function (d) {
        d.x = d3.event.x;
        d.y = d3.event.y;
        d3.select(this)
          .attr('transform', 'translate(' + d.x + ', ' + d.y + ')');
      })
      .on('end', function () {
        if (!d3.event.active) {
          simulation.alphaTarget(0);
        }
      }));

  var circleElements = nodeElements.append('circle')
    .attr('class', 'node-circle')
    .attr('fill', function (d, i) { return color(i); })
    .on('mouseover', function (d) {
      domainTooltip.text(d.domain);
      occurenceTooltip.text(d.count)
      // tooltip.style('visibility', 'visible');
    })
    .on('mousemove', function () {
      // tooltip.style("top", (d3.event.pageY - 10) + "px")
      //   .style("left",(d3.event.pageX + 10) + "px");
    })
    .on("mouseout", function () {
      // tooltip.style("visibility", "hidden");
    });

  nodeElements.append('text')
    .text(function (d) { return (d.count > 1500) ? d.domain : ''; })
    .attr('class', 'node-text');

  simulation.on('tick', function () {
    nodeElements.attr('transform', function (d) { return 'translate(' + d.x + ', ' + d.y + ')'; });
    circleElements.attr('r', function (d) { return scale(d.count); });
  });
});
