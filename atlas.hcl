// Atlas config: generates goose-format migrations from the bun models in src/models.
// Install the CLI (https://atlasgo.io) and run: make atlas-diff name=add_orders
data "external_schema" "bun" {
  program = [
    "go", "run", "-mod=mod", "ariga.io/atlas-provider-bun", "load",
    "--path", "./src/models",
    "--dialect", "postgres",
  ]
}

env "local" {
  src = data.external_schema.bun.url
  dev = "docker://postgres/17/dev?search_path=public"
  migration {
    dir = "file://src/database/migrations?format=goose"
  }
}
