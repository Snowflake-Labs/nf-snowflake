
include { sayHello } from 'plugin/nf-plugin-template'

workflow {
    channel.of('Monde', 'Mondo', 'World', 'Mundo').map { target -> sayHello(target) }
}
