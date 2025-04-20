# Changelog

All notable changes to the BigQuery MCP Server project will be documented in this file.

## [Unreleased]

## [1.0.0] - 2025-04-20

### Added
- 初期実装：Model Context Protocol (MCP) 仕様 rev 2025-03-26に準拠
- stioトランスポート（デフォルト）とHTTPトランスポートのサポート
- BigQuery操作へのアクセスを提供するMCP Toolsの実装
- 長い結果セットのページネーションサポート
- ロギングユーティリティの実装
- JSON-RPC標準に従ったエラーハンドリング
- 簡単な展開のためのDockerサポート
- Claude Desktopとの互換性向上のための直接的なstdioサーバー実装
- `execute_query_with_results`ツール：結果を即時に返すSQLクエリの実行

### Fixed
- DockerコンテナでのClaude Desktop互換性の問題を修正
- stdioトランスポートでコンテナが早期に終了する問題を修正
- INFORMATION_SCHEMAクエリ処理の改善
- サービスアカウント認証情報の処理を改善
- Claude Desktopのレスポンス形式の修正
- 環境変数による設定（プロジェクトIDとロケーション）のサポート追加

### Changed
- FastMCPを使用した実装の最適化
- テストスクリプトを機能別ディレクトリに整理
- 冗長なサーバー実装の削除と整理
