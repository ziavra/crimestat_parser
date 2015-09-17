import tokenize
import token
from StringIO import StringIO


def fixLazyJsonWithComments (in_text):
  """ Same as fixLazyJson but removing comments as well
  """
  result = []
  tokengen = tokenize.generate_tokens(StringIO(in_text).readline)

  sline_comment = False
  mline_comment = False
  last_token = ''

  for tokid, tokval, _, _, _ in tokengen:

    # ignore single line and multi line comments
    if sline_comment:
      if (tokid == token.NEWLINE) or (tokid == tokenize.NL):
        sline_comment = False
      continue

    # ignore multi line comments
    if mline_comment:
      if (last_token == '*') and (tokval == '/'):
        mline_comment = False
      last_token = tokval
      continue

    # fix unquoted strings
    if (tokid == token.NAME):
      if tokval not in ['true', 'false', 'null', '-Infinity', 'Infinity', 'NaN']:
        tokid = token.STRING
        tokval = u'"%s"' % tokval

    # fix single-quoted strings
    elif (tokid == token.STRING):
      if tokval.startswith ("'"):
        tokval = u'"%s"' % tokval[1:-1].replace ('"', '\\"')

    # remove invalid commas
    elif (tokid == token.OP) and ((tokval == '}') or (tokval == ']')):
      if (len(result) > 0) and (result[-1][1] == ','):
        result.pop()

    # detect single-line comments
    elif tokval == "//":
      sline_comment = True
      continue

    # detect multiline comments
    elif (last_token == '/') and (tokval == '*'):
      result.pop() # remove previous token
      mline_comment = True
      continue

    result.append((tokid, tokval))
    last_token = tokval

  return tokenize.untokenize(result)