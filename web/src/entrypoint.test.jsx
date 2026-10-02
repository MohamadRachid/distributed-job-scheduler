import { expect, test } from 'vitest'
import { tokenizeEntrypoint } from './components/SubmitForm.jsx'

test.each([
  [String.raw`python train.py --pattern "\d+\.csv"`, ['python', 'train.py', '--pattern', String.raw`\d+\.csv`]],
  [String.raw`python train.py "C:\data\train.csv"`, ['python', 'train.py', String.raw`C:\data\train.csv`]],
  [String.raw`sh -c "printf \"hello\" && python train.py"`, ['sh', '-c', 'printf "hello" && python train.py']],
  [String.raw`python -c 'print("\n")'`, ['python', '-c', String.raw`print("\n")`]],
  [String.raw`cmd "a\\b" "\$HOME"`, ['cmd', String.raw`a\b`, '$HOME']],
  ['python "" train.py', ['python', '', 'train.py']],
])('preserves arguments for %s', (command, expected) => {
  expect(tokenizeEntrypoint(command)).toEqual(expected)
})

test('rejects unfinished quoted arguments', () => {
  expect(() => tokenizeEntrypoint('python "unfinished')).toThrow(/Unmatched/)
})
