# -*- coding: utf-8 -*-
"""
Content ui specs
=================
"""
import os
from docutils.parsers.rst import Directive, directives
from docutils import nodes
from docutils.statemachine import StringList
from sphinx.util.osutil import copyfile


CSS_FILE = 'contentui.css'
JS_FILE = 'contentui.js'


class ContentTabsDirective(Directive):
    """
    It's container directive with content-tabs class
    """

    has_content = True
    optional_arguments = 1

    def run(self):
        self.assert_has_content()
        text = '\n'.join(self.content)
        node = nodes.container(text)
        node['classes'].append('content-tabs')

        if self.arguments and self.arguments[0]:
            node['classes'].append(self.arguments[0])

        self.add_name(node)
        self.state.nested_parse(self.content, self.content_offset, node)
        return [node]


class ContentTabsContainerDirective(Directive):
    has_content = True
    option_spec = {'title': directives.unchanged}
    required_arguments = 1

    def run(self):
        self.assert_has_content()
        text = '\n'.join(self.content)
        node = nodes.container(text)
        node['ids'].append('tab-%s' % self.arguments[0])
        node['classes'].append('tab-content')

        par = nodes.paragraph(text=self.options["title"])
        par['classes'].append('tab-title')
        node += par

        self.add_name(node)
        self.state.nested_parse(self.content, self.content_offset, node)

        return [node]


class ToggleDirective(Directive):
    has_content = True
    option_spec = {'header': directives.unchanged}
    optional_arguments = 1

    def run(self):
        node = nodes.container()
        node['classes'].append('toggle-content')

        header = self.options["header"]
        par = nodes.paragraph(header)
        par['classes'].append('toggle-header')
        if self.arguments and self.arguments[0]:
            par['classes'].append(self.arguments[0])

        self.state.nested_parse(StringList([header]), self.content_offset, par)
        self.state.nested_parse(self.content, self.content_offset, node)

        return [par, node]


def add_assets(app):
    app.add_stylesheet(CSS_FILE)
    app.add_javascript(JS_FILE)


def copy_assets(app, exception):
    if app.builder.name not in ['html', 'readthedocs'] or exception:
        return
    app.info('Copying contentui stylesheet/javascript... ', nonl=True)
    dest = os.path.join(app.builder.outdir, '_static', CSS_FILE)
    source = os.path.join(os.path.abspath(os.path.dirname(__file__)), CSS_FILE)
    copyfile(source, dest)
    dest = os.path.join(app.builder.outdir, '_static', JS_FILE)
    source = os.path.join(os.path.abspath(os.path.dirname(__file__)), JS_FILE)
    copyfile(source, dest)
    app.info('done')


def setup(app):
    app.add_directive('content-tabs',  ContentTabsDirective)
    app.add_directive('tab-container', ContentTabsContainerDirective)
    app.add_directive('toggle-header', ToggleDirective)
    
    app.connect('builder-inited', add_assets)
    app.connect('build-finished', copy_assets)
